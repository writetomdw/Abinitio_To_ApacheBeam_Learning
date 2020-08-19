import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class SplitIntoFields(beam.DoFn):
    def process(self, element):
        unique_id, price, availability, condition, currency, saletype, merchant, shipping, asins, brand, categories, keys, manufacturer, manufacturerNumber, name = element.split(
            '|')
        return [(unique_id, float(price))]  # usage of dictionary
        # return [(price, id)] # usage of tuple


def FormatOutput(x):
    prod_id = x[0]
    total_price = x[1]
    return "total sale for product id {} is {}".format(prod_id, str(total_price))


# [output_p_coll] = [input_p_coll] | [ Label] >> [Transform]

# Create the Pipeline
with beam.Pipeline(options=PipelineOptions()) as p:
    # lines = p | 'Create' >> beam.Create(['cat|dog', 'snake cat', 'dog']) # example to create data from in-memory

    # Read the input file into the Pipeline skip reading header and and create an output PCollection 1

        pc = ( p | 'Read' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_sold3.csv',
                                               skip_header_lines=1, strip_trailing_newlines=True)
               | "Split into fields" >> (beam.ParDo(SplitIntoFields()))
               | "Sum for each product" >> beam.CombinePerKey(sum)
               )

        # total price for each product sold
        result1 = (pc | "Format to string" >> (beam.Map(FormatOutput))
                     | 'Write' >> beam.io.WriteToText('/Users/wajidm/Downloads/'
                                            , file_name_suffix='.csv'
                                            , shard_name_template='rollup1_test-SSSSS-of-NNNNN')
                   )
        # total price for all sales
        result2 = (pc | "Get the prices" >> beam.Map(lambda element: element[1])
                      | "Combine all prices" >> (beam.CombineGlobally(sum))
                      | "Format the total" >> beam.Map( lambda element: "total sale is {}".format(element))
                      | 'Write Output' >> beam.io.WriteToText('/Users/wajidm/Downloads/'
                                            , file_name_suffix='.csv'
                                            , shard_name_template='rollup2_test-SSSSS-of-NNNNN')
                   )

