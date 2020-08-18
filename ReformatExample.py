import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class SplitIntoFields(beam.DoFn):
    def process(self, element):
        unique_id, price, availability, condition, currency, saletype, merchant, shipping, asins, brand, categories, keys, manufacturer, manufacturerNumber, name = element.split(
            '|')
        return [{"price": price, "id": unique_id}]  # usage of dictionary
        # return [price, id] # usage of tuple


def FormatOutput(product):
    price = product["price"]
    prod_id = product["id"]
    return "price is {} prod id is {}".format(price, prod_id)


# [output_p_coll] = [input_p_coll] | [ Label] >> [Transform]


# Extracting the elements from the p collection is done by  beam SDK
# You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you.

# Transform 0 : Use Pipeline IO Input Operator to read the input rows and form and input starting point PCollection

# Transform 1 : Pipeline the input PCollection in step 1 and do parallel processing with "ParDo" operation to split
#               each element to split each element of the PCollection and reformat to generate desired fields
#               and return a list with output elements ( In this case key value pair dictionary ) for output
#               PCollection.

# Transform 2 : Pipeline the output PCollection in Step 1 as input PCollection in Step 2 and the apply 1 to 1 Map
#               Beam Transform to convert the input PCollection elements into desired String Format
#               output PCollection elements

# Transform 3 : Pipeline the output PCollection in Step 2 into Beam Pipeline IO Write Operation to write the
#               PCollection elements into an output file

#

# Create the Pipeline
with beam.Pipeline(options=PipelineOptions()) as p:
    # lines = p | 'Create' >> beam.Create(['cat|dog', 'snake cat', 'dog']) # example to create data from in-memory

    # Read the input file into the Pipeline skip reading header and and create an output PCollection 1
    pc_1 = p | 'Read' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_sold3.csv',
                                              skip_header_lines=1, strip_trailing_newlines=True)

    # Process the above PCollection1 and separate them into reformatted fields and create a new PCollection 2
    pc_2 = pc_1 | "Split into fields" >> (beam.ParDo(SplitIntoFields()))

    # Format the data in PCollection 2 with a one to one map to text writable format and create a new PCollection 3
    pc_3 = pc_2 | "PairWithOne" >> (beam.Map(FormatOutput))

    # Write the above PCollection 3 using Beam Write component to write to disk
    result = pc_3 | 'Write' >> beam.io.WriteToText('/Users/wajidm/Downloads/'
                                                   , file_name_suffix='.csv'
                                                   , shard_name_template='test-SSSSS-of-NNNNN')
