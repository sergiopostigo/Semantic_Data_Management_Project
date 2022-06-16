import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

        CreateABOX();


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

    public void CreateABOX(){

        // Get AVRO file data
        ArrayList<String> data = OpenAvro("../data/version5.avro");
        Map<String, String> record_map = new HashMap<String, String>();
        data.forEach(record -> {
            record = record.substring(1, record.length() - 1); // remove the first and last characters from the row { }
            String[] pairs = record.split("\", \""); // split the string in the "", "" locations
            for (int i=0;i<pairs.length;i++) {
                String pair = pairs[i];
                String[] keyValue = pair.split(":");
                record_map.put(keyValue[0], keyValue[1]);

            }
        });
        System.out.println(record_map);

        // -- Good --
        ArrayList<String> goods = new ArrayList<String>();



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
}
