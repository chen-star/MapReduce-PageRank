public class Driver {

    public static void main(String[] args) throws Exception {

        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        String transitionMatrix = args[0]; //dir: transition
        String prMatrix = args[1]; //dir: pr
        String subPageRank = args[2]; //dir: subPR
        int count = Integer.parseInt(args[3]); // number of iteration

        for(int i = 0; i < count; i++) {
            String[] args1 = {transitionMatrix, prMatrix+i, subPageRank+i};
            multiplication.main(args1);
            String[] args2 = {subPageRank+i, prMatrix+(i+1)};
            sum.main(args2);
        }
    }
}
