package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.Map;

import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.TCLNode;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;

public class TCLOperator implements Operator {
    private final TCLNode node;
    private final IConcurrencyControlManager ccm;
    private final int txId;
    private boolean executed = false;
    private final IFailureRecoveryManager frm;

    public TCLOperator(TCLNode node, IConcurrencyControlManager ccm, int txId, IFailureRecoveryManager frm) {
        this.node = node;
        this.ccm = ccm;
        this.txId = txId;
        this.frm = frm;
    }

    @Override
    public void open() {
    }

    @Override
    public Row next() {
        if (executed) return null;

        String command = node.command().toUpperCase();
        Map<String, Object> resultData = new HashMap<>();
        
        switch (command) {
            case "BEGIN":
            case "BEGIN TRANSACTION":
                resultData.put("status", "Transaction already started");
                resultData.put("transaction_id", txId);
                System.out.println("[TCL] BEGIN TRANSACTION: txId=" + txId);
                break;
                
            case "COMMIT":
                ccm.endTransaction(txId, true);
                resultData.put("status", "Transaction committed");
                resultData.put("transaction_id", txId);
                // frm.writeTransactionLog(txId, "COMMIT");
                System.out.println("[TCL] COMMIT: txId=" + txId);
                break;
                
            case "ROLLBACK":
            case "ABORT":
                ccm.endTransaction(txId, false);
                resultData.put("status", "Transaction rolled back");
                resultData.put("transaction_id", txId);
                // frm.writeTransactionLog(txId, "ABORT");
                frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", String.valueOf(txId), null));
                System.out.println("[TCL] ROLLBACK: txId=" + txId);
                break;
                
            default:
                throw new UnsupportedOperationException("Unsupported TCL command: " + command);
        }
        
        executed = true;
        return new Row(resultData);
    }

    @Override
    public void close() {
    }
}
