package shadow.pgsql;

/**
* Created by zilence on 10.08.14.
*/
public enum TransactionStatus {
    IDLE, // not in a transaction
    TRANSACTION,
    FAILED // failed transaction
}
