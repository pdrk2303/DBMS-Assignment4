import convention.PConvention;
import rules.PRules;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.RelNode;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class SFWTest {
    @Test 
    public void testSFW() {
        try{
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            SqlNode sqlNode = calciteConnection.parseSql("select first_name from actor where actor_id > 100 and actor_id < 150");
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("[+] RelNode tree: \n" + relNode.explain());

            RuleSet rules = RuleSets.ofList(
                PRules.P_PROJECT_RULE,
                PRules.P_FILTER_RULE
                // PRules.P_TABLESCAN_RULE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("[+] Physical SQL: \n" + phyRelNode);

            System.out.println("[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.executeQuery(phyRelNode);

            if(result == null) {
                System.out.println("[-] No result found");
            }
            else{
                System.out.println("[+] Final Output : ");
                for (Object [] row : result) {
                    for (Object col : row) {
                        System.out.print(col + " ");
                    }
                    System.out.println();
                }
            }
            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test passed :)");
        return;
    }
}
