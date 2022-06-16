public class Main {

    // URL
    static String SOURCE = "http://dataimporta.com/";

    public static void main(String[] args) {

        KnowledgeGraph kg = new KnowledgeGraph(SOURCE);
        //kg.showClasses();
        //kg.showClassesAndInstances();
        kg.showClassesInstancesAndProperties();

    }
}
