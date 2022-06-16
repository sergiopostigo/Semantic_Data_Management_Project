import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.util.iterator.ExtendedIterator;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class KnowledgeGraph {

    //Knowledge Graph Model
    OntModel kg_model;

    // Constructor
    public KnowledgeGraph(String SOURCE) {

        // Ontology namespace
        String NS = SOURCE + "#";

        // Create the base model
        kg_model = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);

        // Create the TBOX
        CreateTBOX(NS);

        CreateABOX(NS);


        //ArrayList<String> iu = OpenAvro("../data/version5.avro");
        //System.out.println(iu.get(0));



    }

    public void CreateTBOX(String NS){

        // Creating classes and subclasses
        OntClass good = kg_model.createClass(NS + "Good");
        OntClass customs_description = kg_model.createClass(NS + "Customs_Description");
        OntClass via = kg_model.createClass(NS + "Via");
        OntClass country = kg_model.createClass(NS + "Country");
        OntClass company = kg_model.createClass(NS + "Company");
        OntClass trader = kg_model.createClass(NS + "Trader");
        OntClass shipper = kg_model.createClass(NS + "Shipper");
        company.addSubClass(shipper);
        company.addSubClass(trader);

        // Creating Object Type Properties
        ObjectProperty belongs_to = kg_model.createObjectProperty(NS + "belongs_to");
        belongs_to.addDomain(good);
        belongs_to.addRange(customs_description);

        ObjectProperty sent_by = kg_model.createObjectProperty(NS + "sent_by");
        sent_by.addDomain(good);
        sent_by.addRange(via);

        ObjectProperty comes_from = kg_model.createObjectProperty(NS + "comes_from");
        comes_from.addDomain(good);
        comes_from.addRange(country);

        ObjectProperty goes_to = kg_model.createObjectProperty(NS + "goes_to");
        goes_to.addDomain(good);
        goes_to.addRange(country);

        ObjectProperty transports = kg_model.createObjectProperty(NS + "transports");
        transports.addDomain(shipper);
        transports.addRange(good);

        ObjectProperty imports = kg_model.createObjectProperty(NS + "imports");
        imports.addDomain(trader);
        imports.addRange(good);

        ObjectProperty exports = kg_model.createObjectProperty(NS + "exports");
        exports.addDomain(trader);
        exports.addRange(good);

        // Creating Data Type Properties
        DatatypeProperty date = kg_model.createDatatypeProperty(NS +"date");
        date.addDomain(good);

        DatatypeProperty cost = kg_model.createDatatypeProperty(NS +"cost");
        cost.addDomain(good);

        DatatypeProperty weight = kg_model.createDatatypeProperty(NS +"weight");
        weight.addDomain(good);

        DatatypeProperty commercial_description = kg_model.createDatatypeProperty(NS +"commercial_description");
        commercial_description.addDomain(good);

        DatatypeProperty country_name = kg_model.createDatatypeProperty(NS +"name");
        country_name.addDomain(country);

        DatatypeProperty company_name = kg_model.createDatatypeProperty(NS +"name");
        company_name.addDomain(company);

        DatatypeProperty text = kg_model.createDatatypeProperty(NS +"text");
        text.addDomain(customs_description);


    }

    public void CreateABOX(String NS){

        // Path to data
        String data_path = "../data/peru_data.csv";
        // Read the data
        List<String[]>  data = readCSV(data_path);

        // ---------------------------------------
        // Trader
        // ---------------------------------------
        ArrayList<String> traders = new ArrayList<String>();
        // Get all traders
        data.forEach(record -> {
            traders.add(record[6]);
        });
        // Remove duplicated traders
        Set<String> tradersWithoutDuplicates = new LinkedHashSet<String>(traders);
        traders.clear();
        traders.addAll(tradersWithoutDuplicates);
        // Create individuals
        traders.forEach(trader -> {
            // Get the class
            OntClass traderClass = kg_model.getOntClass( NS + "Trader" );
            // Generate a random id
            Random rnd = new Random();
            long id = Instant.now().toEpochMilli()+rnd.nextInt(9999999);
            // Create the instance
            Individual goodIndividual = kg_model.createIndividual(NS + "Trader/" + id, traderClass);
            // Create the corresponding literals
            Literal nameLiteral = kg_model.createLiteral(trader);
            // Link the instance with the literals
            goodIndividual.addProperty(kg_model.getProperty(NS +"name"), nameLiteral);

        });
        // ---------------------------------------


    }

    public ArrayList<String> OpenAvro(String path) {

        ArrayList<String> data = new ArrayList<String>();
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(path), datumReader);
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                data.add(record.toString());
            }
        } catch (IOException e) {
            System.out.println("File not found");;
        }

        return data;
    }

    private List<String[]> readCSV(String path){

        List<String[]> list = null;
        try {
            FileReader filereader = new FileReader(path);
            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            CSVReader csvReader = new CSVReaderBuilder(filereader)
                    .withCSVParser(parser)
                    .withSkipLines(1)
                    .build();

            // Read all data at once
            list = csvReader.readAll();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public void showClasses() {

        // Create an iterator object containing all classes in the TBOX
        ExtendedIterator classes = kg_model.listClasses();

        // Iterate through all the classes
        while (classes.hasNext()) {
            OntClass thisClass = (OntClass) classes.next();
            // Print the found class
            System.out.println("Found class: " + thisClass.toString());

        }

    }
    public void showClassesAndInstances() {

        // Create an iterator object containing all classes in the TBOX
        ExtendedIterator classes = kg_model.listClasses();

        // Iterate through all the classes
        while (classes.hasNext()) {
            OntClass thisClass = (OntClass) classes.next();
            // Print the found class
            System.out.println("Found class: " + thisClass.toString());

            // Create an iterator object containing all instances (also called individuals) of the current class
            ExtendedIterator instances = thisClass.listInstances();

            // Iterate through all the instances
            while (instances.hasNext()) {
                Individual thisInstance = (Individual) instances.next();
                // Print the found instance
                System.out.println("  Found instance: " + thisInstance.toString());

            }
        }

    }
    public void showClassesInstancesAndProperties() {

        // Create an iterator object containing all classes in the TBOX
        ExtendedIterator classes = kg_model.listClasses();

        // Iterate through all the classes
        while (classes.hasNext()) {
            OntClass thisClass = (OntClass) classes.next();
            // Print the found class
            System.out.println("Found class: " + thisClass.toString());

            // Create an iterator object containing all instances (also called individuals) of the current class
            ExtendedIterator instances = thisClass.listInstances();

            // Iterate through all the instances
            while (instances.hasNext()) {
                Individual thisInstance = (Individual) instances.next();
                // Print the found instance
                System.out.println("  Found instance: " + thisInstance.toString());

                // Create an iterator object containing all instances (also called individuals) of the current property
                ExtendedIterator properties = thisInstance.listProperties();

                // Iterate through all the properties
                while (properties.hasNext()) {
                    Statement thisProperty = (Statement) properties.next();
                    // Print the found property
                    System.out.println("    Found property: " + thisProperty.toString());

                }

            }
        }

    }
}


