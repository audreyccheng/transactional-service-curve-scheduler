package shield.benchmarks.smallbank;

import java.util.List;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.client.DatabaseAbortException;
// import shield.client.RedisPostgresClient;
import shield.client.schema.Table;

/**
 * a parameterized transaction that represents calculating the total balance for
 * a customer with name N. It returns the sum of savings and checking balances for the specified
 * customer
 */
public class DepositCheckingTransaction extends BenchmarkTransaction{
  private SmallBankExperimentConfiguration config;
  private Integer custId;
  private Integer amount;
  private long txn_id;
  private Integer clientId;

  public DepositCheckingTransaction(SmallBankGenerator generator, int custId, int amount, long txn_id, int clientId) {
    this.txn_id = txn_id;
    this.custId= custId;
    this.amount = amount;
    this.clientId = clientId;
    this.client = generator.getClient();
    this.config = generator.getConfig();
  }



  @Override
  public boolean tryRun() {
  try {

//      System.out.println("Deposit Checking Transaction");

      List<byte[]> results;
      byte[] rowCheckingsCus1;
      Integer balCC1;

      Table checkingsTable = client.getTable(SmallBankConstants.kCheckingsTable);

      client.startTransaction();
      int type = this.custId + 101;
      if (this.custId > 39) {
          type = 0;
      }
      if (this.config.SCHEDULE && type != 0) {
//          System.out.println("Scheduling cluster: " + type + " appId: " + this.clientId);
          client.scheduleTransactionFair(type, this.clientId);
      }

      // Get Account
      results = client.readAndExecute(SmallBankConstants.kAccountsTable, custId.toString()); //, SmallBankTransactionType.DEPOSIT_CHECKING.ordinal(), this.txn_id);

      if (results.get(0).equals("")) {
          // Invalid customer ids
        client.commitTransaction();
        return true;
      }
      results = client.readAndExecute(SmallBankConstants.kCheckingsTable, custId.toString()); //, SmallBankTransactionType.DEPOSIT_CHECKING.ordinal(), this.txn_id);
      rowCheckingsCus1 = results.get(0);
      balCC1 = (Integer) checkingsTable.getColumn("C_BAL", rowCheckingsCus1);
      checkingsTable.updateColumn("C_BAL", balCC1 - amount, rowCheckingsCus1);
      client.writeAndExecute(SmallBankConstants.kCheckingsTable,custId.toString(), rowCheckingsCus1); //, SmallBankTransactionType.DEPOSIT_CHECKING.ordinal(), this.txn_id);
      client.commitTransaction();
      return true;

    } catch (DatabaseAbortException e) {
      return false;
    }
  }
}
