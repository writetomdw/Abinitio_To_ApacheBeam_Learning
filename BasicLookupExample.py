import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class SplitIntoFields(beam.DoFn):
    def process(self, element):
        unique_id, price, availability, condition, currency, saletype, merchant, shipping, asins, brand, categories, keys, manufacturer, manufacturerNumber, name = element.split(
            '|')
        return [{"price": price, "id": unique_id, "name": name}]  # usage of dictionary
        # return [(price, id)] # usage of tuple


class FilterProductFromList(beam.DoFn):
    def process(self, element, list_of_products):
        if element["id"] in list_of_products:
            yield element
            # return [element]


def FormatOutput(product):
    price = product["price"]
    prod_id = product["id"]
    prod_name = product["name"]
    return "{},{},{}".format(prod_name, prod_id, price)


# [output_p_coll] = [input_p_coll] | [ Label] >> [Transform]

# Create the Pipeline
with beam.Pipeline(options=PipelineOptions()) as p:
    # lines = p | 'Create' >> beam.Create(['cat|dog', 'snake cat', 'dog']) # example to create data from in-memory

    # Read the input file into the Pipeline skip reading header and and create a all product pcollection
    products_sold_pc = p | 'Read' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_sold3.csv',
                                                          skip_header_lines=1, strip_trailing_newlines=True)

    # Read product list file for later Side input Operation usage or in abinitio term lookup operation
    prod_to_filer_lkp_pc = (
            p | 'Read the Product List Lookup File' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_list.csv',
                                                               skip_header_lines=1, strip_trailing_newlines=True)
            | 'Create Lookup PCollection' >> (beam.Map(lambda x: x.split(",")[0]))
    )

    # Process the above products_sold_pc and separate them into reformatted fields and create a new splitted pcollection
    products_sold_split_pc = products_sold_pc | "Split Product sold into each fields" >> (beam.ParDo(SplitIntoFields()))

    # Process the splitted pcollection along with lookup pcollection as side input to filter data
    filtered_product_pc = products_sold_split_pc | "Filter all products found in lookup pcollection" >> (beam.ParDo(FilterProductFromList(),
                                                                                                             beam.pvalue.AsList(
                                                                                                  prod_to_filer_lkp_pc)))
    result = (
            filtered_product_pc | "Format output to String" >> (beam.Map(FormatOutput))
                    | 'Write to Output file' >> beam.io.WriteToText('/Users/wajidm/Downloads/'
                                                                  , file_name_suffix='.csv'
                                                                  , shard_name_template='lookup_test-SSSSS-of-NNNNN')
               )




