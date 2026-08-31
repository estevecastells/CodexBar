import Foundation

extension CostUsageStore {
    /// Only the actor can resolve this receipt. It carries no decoded state or SQLite handle.
    final class CodexBaselineReceipt: Sendable {
        fileprivate let id = UUID()
        private let store: CostUsageStore

        fileprivate init(store: CostUsageStore) {
            self.store = store
        }

        deinit {
            let store = self.store
            let id = self.id
            Task { await store.releaseCodexBaseline(id: id) }
        }
    }

    struct CodexPersistenceState {
        var metadata: CostUsageStoreMetadata
        var files: [CostUsageStoreFile]
        var snapshotCounts: [String: Int]
        var rowCounts: [String: Int]
        var fileDayAggregates: [CostUsageStoreFileDayAggregate]

        init(
            snapshot: CostUsageStoreSnapshot,
            snapshotCounts: [String: Int]? = nil,
            rowCounts: [String: Int]? = nil)
        {
            self.metadata = snapshot.metadata
            self.files = snapshot.files.map { file in
                var file = file
                // Resume bodies already belong to the typed cache. Keep the compact details
                // payload because lazy scanner baselines need its history-presence flags when
                // saving files whose histories were deliberately left unloaded.
                file.scanState.resumePayload = nil
                return file
            }
            self.snapshotCounts = snapshotCounts
                ?? snapshot.tokenSnapshots.reduce(into: [:]) { $0[$1.path, default: 0] += 1 }
            self.rowCounts = rowCounts
                ?? snapshot.usageRows.reduce(into: [:]) { $0[$1.path, default: 0] += 1 }
            self.fileDayAggregates = snapshot.fileDayAggregates
        }
    }

    struct CodexDecodedBaseline {
        var decoded: CostUsageCache
        var persistence: CodexPersistenceState
        var stamp: DatabaseStamp
        var unloadedTokenSnapshotPaths: Set<String>
        var unloadedUsageRowPaths: Set<String>
        var legacyTurnIDSummaryPaths: Set<String>
        var historiesLoaded: Bool
    }

    struct RetainedCodexBaseline {
        var id: UUID
        var baseline: CodexDecodedBaseline
    }

    func loadCodexScan(calendar: Calendar) -> CostUsageStoreLoad {
        self.retainedCodexBaseline = nil
        _ = self.removeLegacyCodexArtifactIfPresent()
        let receipt = CodexBaselineReceipt(store: self)
        guard let baseline = self.readCodexBaseline() else {
            // Keep a receipt even on failure so save cannot fall back to accepting unbased content.
            return CostUsageStoreLoad(store: self, cache: CostUsageCache(), receipt: receipt)
        }
        self.retainedCodexBaseline = RetainedCodexBaseline(id: receipt.id, baseline: baseline)
        let compatible = baseline.decoded.timeZoneIdentifier == nil
            || baseline.decoded.timeZoneIdentifier == calendar.timeZone.identifier
        let cache = compatible ? Self.reconciledCodexCache(
            baseline.decoded, persistence: baseline.persistence) : CostUsageCache()
        return CostUsageStoreLoad(
            store: self,
            cache: cache,
            receipt: receipt,
            unloadedTokenSnapshotPaths: compatible ? baseline.unloadedTokenSnapshotPaths : [],
            unloadedUsageRowPaths: compatible ? baseline.unloadedUsageRowPaths : [],
            legacyTurnIDSummaryPaths: compatible ? baseline.legacyTurnIDSummaryPaths : [])
    }

    func releaseCodexBaseline(_ receipt: CodexBaselineReceipt) {
        self.releaseCodexBaseline(id: receipt.id)
    }

    private func releaseCodexBaseline(id: UUID) {
        if self.retainedCodexBaseline?.id == id {
            self.retainedCodexBaseline = nil
            #if DEBUG
            let observer = self.codexBaselineReleaseObserverForTesting
            self.codexBaselineReleaseObserverForTesting = nil
            observer?()
            #endif
        }
    }

    func takeCodexBaseline(_ receipt: CodexBaselineReceipt?) -> CodexDecodedBaseline? {
        guard let receipt else {
            self.retainedCodexBaseline = nil
            return self.readCodexBaseline(loadHistories: true)
        }
        guard self.retainedCodexBaseline?.id == receipt.id else { return nil }
        defer { self.retainedCodexBaseline = nil }
        return self.retainedCodexBaseline?.baseline
    }

    func readCodexBaseline(loadHistories: Bool = false) -> CodexDecodedBaseline? {
        let baseline: CodexDecodedBaseline? = self.withDatabase(default: nil) { database in
            guard let before = try? self.databaseStamp(database) else { return nil }
            let persisted = try? Self.inReadTransaction(database) {
                let snapshot = try Self.readSnapshot(
                    database,
                    tokenSnapshotPaths: loadHistories ? nil : [],
                    usageRowPaths: loadHistories ? nil : [],
                    recorder: self.scopedReadWorkRecorderForTesting)
                let rowCounts = try Self.readUsageRowCounts(database)
                #if DEBUG
                if let checkpoint = Self.codexBaselineReadCheckpointForTesting,
                   checkpoint.databaseURL == self.databaseURL
                {
                    try checkpoint.checkpoint()
                }
                #endif
                return (snapshot, rowCounts)
            }
            // data_version inside the read transaction can still describe its pinned snapshot.
            // Compare after COMMIT; never attach a newer version to the old decoded rows.
            guard let persisted, let after = try? self.databaseStamp(database), before == after else { return nil }
            return self.makeCodexBaseline(
                snapshot: persisted.0,
                rowCounts: persisted.1,
                stamp: after,
                historiesLoaded: loadHistories)
        }
        if baseline == nil {
            // Uncertain reads may be racing schema changes. Preserve the database and drain any
            // failed read transaction; a fresh open still owns normal integrity/recovery checks.
            self.recoverConnectionAfterFailure()
        }
        return baseline
    }

    private func makeCodexBaseline(
        snapshot: CostUsageStoreSnapshot,
        rowCounts: [String: Int],
        stamp: DatabaseStamp,
        historiesLoaded: Bool = false) -> CodexDecodedBaseline
    {
        var unloadedTokenSnapshotPaths: Set<String> = []
        var unloadedUsageRowPaths: Set<String> = []
        var legacyTurnIDSummaryPaths: Set<String> = []
        return CodexDecodedBaseline(
            decoded: Self.decodeCodexCache(
                from: snapshot,
                recorder: self.scopedReadWorkRecorderForTesting,
                loadedTokenSnapshotPaths: historiesLoaded ? nil : [],
                loadedUsageRowPaths: historiesLoaded ? nil : [],
                unloadedTokenSnapshotPathRecorder: { unloadedTokenSnapshotPaths.insert($0) },
                unloadedUsageRowPathRecorder: { unloadedUsageRowPaths.insert($0) },
                legacyTurnIDSummaryPathRecorder: { legacyTurnIDSummaryPaths.insert($0) }),
            persistence: CodexPersistenceState(
                snapshot: snapshot,
                snapshotCounts: historiesLoaded ? nil : Dictionary(uniqueKeysWithValues: snapshot.accumulators.map {
                    ($0.path, $0.eventCount)
                }),
                rowCounts: historiesLoaded ? nil : rowCounts),
            stamp: stamp,
            unloadedTokenSnapshotPaths: unloadedTokenSnapshotPaths,
            unloadedUsageRowPaths: unloadedUsageRowPaths,
            legacyTurnIDSummaryPaths: legacyTurnIDSummaryPaths,
            historiesLoaded: historiesLoaded)
    }

    func codexBaselineIsCurrent(_ baseline: CodexDecodedBaseline) -> Bool {
        self.currentDatabaseStamp() == baseline.stamp
    }

    /// Retention may rewrite identical metadata when a protected window exceeds the budget.
    /// Only those own writes permit a fresh locked semantic comparison; external changes retry.
    func codexBaselineAfterRetention(_ baseline: CodexDecodedBaseline) -> CodexDecodedBaseline? {
        self.withDatabase(default: nil) { database in
            guard let current = self.currentDatabaseStamp() else { return nil }
            if current == baseline.stamp {
                return baseline
            }
            var original = baseline.stamp
            original.totalChanges = current.totalChanges
            guard original == current else { return nil }
            let snapshot = try Self.readSnapshot(
                database,
                tokenSnapshotPaths: baseline.historiesLoaded ? nil : [],
                usageRowPaths: baseline.historiesLoaded ? nil : [],
                recorder: self.scopedReadWorkRecorderForTesting)
            let rowCounts = try Self.readUsageRowCounts(database)
            return self.makeCodexBaseline(
                snapshot: snapshot,
                rowCounts: rowCounts,
                stamp: current,
                historiesLoaded: baseline.historiesLoaded)
        }
    }

    #if DEBUG
    nonisolated(unsafe) static var codexBaselineReadCheckpointForTesting: (
        databaseURL: URL,
        checkpoint: () throws -> Void)?

    var retainedCodexBaselineCountForTesting: Int {
        self.retainedCodexBaseline == nil ? 0 : 1
    }
    #endif
}
