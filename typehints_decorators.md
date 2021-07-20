```python
# import beam module
import apache_beam as beam

p = beam.Pipeline()

#this is a decorator 
#you can also you output instead of input
@beam.typehints.with_input_types(int)
class FilterEvensDoFn(beam.DoFn):
  def process(self, element):
    if element % 2 == 0:
      yield element

evens = ( p
         | beam.Create(['1','2','3'])
         | beam.ParDo(FilterEvensDoFn()) 
        )
  
p.run()



import apache_beam as beam

p = beam.Pipeline()

evens = ( p 
         | beam.Create(['one','two','three']) 
         | beam.Filter(lambda x: x % 2 == 0).with_input_types(int) 
        )
  
p.run()
```

    WARNING:apache_beam.runners.interactive.interactive_environment:Dependencies required for Interactive Beam PCollection visualization are not available, please use: `pip install apache-beam[interactive]` to install necessary dependencies to enable all data visualization features.



    ---------------------------------------------------------------------------

    TypeCheckError                            Traceback (most recent call last)

    <ipython-input-1-cd6b5c5cecb3> in <module>
         14 evens = ( p
         15          | beam.Create(['1','2','3'])
    ---> 16          | beam.ParDo(FilterEvensDoFn())
         17         )
         18 


    /opt/conda/lib/python3.7/site-packages/apache_beam/pvalue.py in __or__(self, ptransform)
        136 
        137   def __or__(self, ptransform):
    --> 138     return self.pipeline.apply(ptransform, self)
        139 
        140 


    /opt/conda/lib/python3.7/site-packages/apache_beam/pipeline.py in apply(self, transform, pvalueish, label)
        573     type_options = self._options.view_as(TypeOptions)
        574     if type_options.pipeline_type_check:
    --> 575       transform.type_check_inputs(pvalueish)
        576 
        577     pvalueish_result = self.runner.apply(transform, pvalueish, self._options)


    /opt/conda/lib/python3.7/site-packages/apache_beam/transforms/ptransform.py in type_check_inputs(self, pvalueish)
        863           raise TypeCheckError(
        864               'Type hint violation for \'%s\': requires %s but got %s for %s'
    --> 865               % (self.label, hint, bindings[arg], arg))
        866 
        867   def _process_argspec_fn(self):


    TypeCheckError: Type hint violation for 'Cell 1: ParDo(FilterEvensDoFn)': requires <class 'int'> but got <class 'str'> for element



```python

```
