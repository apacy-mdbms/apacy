public class MockFailureRecoveryManager implements IFailureRecoveryManager {

    @Override
    public void writeLog(ExecutionResult info) {
        System.out.println("[MOCK-FRM] writeLog: Tx " + info.transactionId() + " | Op: " + info.operation()
                + " | Status: " + info.success());
    }

    @Override
    public void writeTransactionLog(int transactionId, String lifecycleEvent) {
        System.out.println("[MOCK-FRM] writeTransactionLog: Tx " + transactionId + " | Event: " + lifecycleEvent);
    }

    @Override
    public void saveCheckpoint() {
        System.out.println("[MOCK-FRM] saveCheckpoint dipanggil.");
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        System.out.println("[MOCK-FRM] RECOVER dipanggil. Type: " + criteria.recoveryType() + ", TxId: "
                + criteria.transactionId());
    }
}