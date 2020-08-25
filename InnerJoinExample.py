import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class SplitProductsSold(beam.DoFn):
    def process(self, element):
        unique_id, price, availability, condition, currency, saletype, merchant, shipping, asins, brand, categories, keys, manufacturer, manufacturerNumber, name = element.split(
            '|')
        return [(unique_id, float(price))]  # usage of dictionary
        # return [(price, id)] # usage of tuple

class SplitProductList(beam.DoFn):
    def process(self, element):
        unique_id, colour = element.split(',')
        return [(unique_id, colour)]

class FilterColouredProduct(beam.DoFn):
    def process(self, element):
        if len(element[1]["colour"]) > 0:
            return [element]

def FormatOutput(x):
    prod_id = x[0]
    total_price = x[1]
    return "total sale for product id {} is {}".format(prod_id, str(total_price))

with beam.Pipeline(options=PipelineOptions()) as p:
    pc_prod_sold = ( p | 'Read Product Sold' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_sold3.csv',
                                               skip_header_lines=1, strip_trailing_newlines=True)
               | "Split Products Sold File" >> (beam.ParDo(SplitProductsSold()))
               )

    pc_prod_list = (p | 'Read Product List' >> beam.io.ReadFromText('/Users/wajidm/Downloads/product_list.csv',
                                                   skip_header_lines=1, strip_trailing_newlines=True)
                | "Split Products List File" >> (beam.ParDo(SplitProductList()))
                )

    # Join products
    result1 = (
                ({"sold_price": pc_prod_sold, "colour": pc_prod_list})
                | "Join with keys" >> beam.CoGroupByKey()
                | "Filter products with colour" >> beam.ParDo(FilterColouredProduct())
                | beam.Map(lambda x : "{},{},{}".format(x[0],str(x[1]["sold_price"]),str(x[1]["colour"])))
                | beam.Map(print)
        )
