import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class SplitIntoFields(beam.DoFn):
    def process(self, element):
        unique_id, price, availability, condition, currency, saletype, merchant, shipping, asins, brand, categories, keys, manufacturer, manufacturerNumber, name = element.split(
            '|')
        return [{"price": price, "id": unique_id, "name": name}]  # usage of dictionary
        # return [(price, id)] # usage of tuple

class FilterPrice(beam.DoFn):
    def process(self, element):
        if float(element["price"]) > 100.00:
            return [element]  # usage of dictionary

def FormatOutput(product):
    price = product["price"]
    prod_id = product["id"]
    prod_name = product["name"]
    return "{},{},{}".format(prod_name, prod_id ,price)


# [output_p_coll] = [input_p_coll] | [ Label] >> [Transform]

# Create the Pipeline
with beam.Pipeline(options=PipelineOptions()) as p:
    # lines = p | 'Create' >> beam.Create(['cat|dog', 'snake cat', 'dog']) # example to create data from in-memory

    # Read the input file into the Pipeline skip reading header and and create an output PCollection 1
    pc_1 = p | 'Read' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_sold3.csv',
                                              skip_header_lines=1, strip_trailing_newlines=True)

    # Process the above PCollection1 and separate them into reformatted fields and create a new PCollection 2
    pc_2 = pc_1 | "Split into fields" >> (beam.ParDo(SplitIntoFields()))

    # Process the above PCollection1 and separate them into reformatted fields and create a new PCollection 2
    pc_3 = pc_2 | "Filter the price higher than 100" >> (beam.ParDo(FilterPrice()))

    # Format the data in PCollection 2 with a one to one map to text writable format and create a new PCollection 3
    pc_4 = pc_3 | "Format to String" >> (beam.Map(FormatOutput))

    # Write the above PCollection 3 using Beam Write component to write to disk
    result = pc_4 | 'Write' >> beam.io.WriteToText('/Users/wajidm/Downloads/'
                                                   , file_name_suffix='.csv'
                                                   , shard_name_template='filter_test-SSSSS-of-NNNNN')
