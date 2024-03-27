import convention.PConvention;
import rules.PRules;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.RelNode;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class OperatorsTest {
    @Test 
    public void test() {
        try{
            List<String> queries = new ArrayList<>();

            // Uncomment queries to test

            // queries.add("SELECT COUNT(*) FROM (SELECT rental.rental_id, rental.rental_date, payment.amount\n"
            //     + "FROM rental\n"
            //     + "JOIN payment ON rental.rental_id = payment.rental_id)");

            // queries.add("select film_id, count(film_id) as cnt\n"
            //     + "from rental r join inventory i\n"
            //     + "on r.inventory_id = i.inventory_id\n"
            //     + "group by film_id order by cnt DESC");

            // queries.add("select f.title from\n"
            //     + "(select title, length from film where rating = 'PG')\n"
            //     + "as f where f.length < 50 order by f.title DESC");

            for (String query : queries) {

                MyCalciteConnection calciteConnection = new MyCalciteConnection();
                SqlNode sqlNode = calciteConnection.parseSql(query);
                System.out.println("[+] Parsed SQL: \n" + sqlNode);
                SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
                System.out.println("[+] Validated SQL: \n" + validatedSqlNode);
                RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
                System.out.println("[+] RelNode tree: \n" + relNode.explain());

                RuleSet rules = RuleSets.ofList(
                    PRules.P_PROJECT_RULE,
                    PRules.P_FILTER_RULE,
                    PRules.P_TABLESCAN_RULE,
                    PRules.P_JOIN_RULE,
                    PRules.P_AGGREGATE_RULE,
                    PRules.P_SORT_RULE
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
                    calciteConnection.close();
                    continue;
                }

                System.out.println("[+] Final Output : ");
                for (Object [] row : result) {
                    for (Object col : row) {
                        System.out.print(col + " ");
                    }
                    System.out.println();
                }

                calciteConnection.close();
            }
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
