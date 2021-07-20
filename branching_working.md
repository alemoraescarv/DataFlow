```python
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery

def SplitRow(element):
    return element.split(',')

p = beam.Pipeline()


input_collection = ( 
                      p 
                      | "Read from text file" >> beam.io.ReadFromText('dept_data.txt')
                      | "Split rows" >> beam.Map(SplitRow)
                   )

table_schema = 'col1:STRING, col2:INT64'
table_spec = 'test-proj-261014:bq_load_codelab.table_df'
accounts_count = (
                      input_collection
                      | 'Get all Accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
                      | 'Pair each accounts employee with 1' >> beam.Map(lambda record: ("Accounts, " +record[1], 1))
                      | 'Group and sum1' >> beam.CombinePerKey(sum)
                      | 'Write results for account' >> beam.io.WriteToText('data/Account')
                      | 'Write to BQ' >>  beam.io.WriteToBigQuery(
                                                                            table_spec,
                                                                            #schema=table_schema,
                                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

                 )

hr_count = (
                input_collection
                | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
                | 'Pair each hr employee with 1' >> beam.Map(lambda record: ("HR, " +record[1], 1))
                | 'Group and sum' >> beam.CombinePerKey(sum)
                | 'Write results for hr' >> beam.io.WriteToText('data/HR')
           )

output =(
         (accounts_count,hr_count)
    | beam.Flatten()
    | beam.io.WriteToText('data/both')
)



p.run()
  
# Sample the first 20 results, remember there are no ordering guarantees.
!{('head -n 20 data/both-00000-of-00001')}


#!{('head -n 20 data/HR-00000-of-00001')}
```

    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py:1421: BeamDeprecationWarning: options is deprecated since First stable release. References to <pipeline>.options will not be supported
      experiments = p.options.view_as(DebugOptions).experiments or []
    WARNING:apache_beam.io.filebasedsink:Deleting 1 existing files in target path matching: -*-of-%(num_shards)05d
    WARNING:apache_beam.io.gcp.bigquery_tools:Sleeping for 150 seconds before the write as BigQuery inserts can be routed to deleted table for 2 mins after the delete and create.



    ---------------------------------------------------------------------------

    AttributeError                            Traceback (most recent call last)

    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner._invoke_bundle_method()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnInvoker.invoke_finish_bundle()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnInvoker.invoke_finish_bundle()


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in finish_bundle(self)
       1036   def finish_bundle(self):
    -> 1037     return self._flush_all_batches()
       1038 


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in _flush_all_batches(self)
       1043     return itertools.chain(*[self._flush_batch(destination)
    -> 1044                              for destination in list(self._rows_buffer.keys())
       1045                              if self._rows_buffer[destination]])


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in <listcomp>(.0)
       1044                              for destination in list(self._rows_buffer.keys())
    -> 1045                              if self._rows_buffer[destination]])
       1046 


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in _flush_batch(self, destination)
       1069           insert_ids=insert_ids,
    -> 1070           skip_invalid_rows=True)
       1071 


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery_tools.py in insert_rows(self, project_id, dataset_id, table_id, rows, insert_ids, skip_invalid_rows)
        836     for i, row in enumerate(rows):
    --> 837       json_row = self._convert_to_json_row(row)
        838       insert_id = str(self.unique_row_id) if not insert_ids else insert_ids[i]


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery_tools.py in _convert_to_json_row(self, row)
        846     json_object = bigquery.JsonObject()
    --> 847     for k, v in iteritems(row):
        848       if isinstance(v, decimal.Decimal):


    /opt/conda/lib/python3.7/site-packages/future/utils/__init__.py in iteritems(obj, **kwargs)
        310     if not func:
    --> 311         func = obj.items
        312     return func(**kwargs)


    AttributeError: 'str' object has no attribute 'items'

    
    During handling of the above exception, another exception occurred:


    AttributeError                            Traceback (most recent call last)

    <ipython-input-15-fb016d27c24b> in <module>()
         46 
         47 
    ---> 48 p.run()
         49 
         50 # Sample the first 20 results, remember there are no ordering guarantees.


    /opt/conda/lib/python3.7/site-packages/apache_beam/pipeline.py in run(self, test_runner_api)
        459           self.to_runner_api(use_fake_coders=True),
        460           self.runner,
    --> 461           self._options).run(False)
        462 
        463     if self._options.view_as(TypeOptions).runtime_type_check:


    /opt/conda/lib/python3.7/site-packages/apache_beam/pipeline.py in run(self, test_runner_api)
        472       finally:
        473         shutil.rmtree(tmpdir)
    --> 474     return self.runner.run_pipeline(self, self._options)
        475 
        476   def __enter__(self):


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/direct/direct_runner.py in run_pipeline(self, pipeline, options)
        180       runner = BundleBasedDirectRunner()
        181 
    --> 182     return runner.run_pipeline(pipeline, options)
        183 
        184 


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in run_pipeline(self, pipeline, options)
        484 
        485     self._latest_run_result = self.run_via_runner_api(pipeline.to_runner_api(
    --> 486         default_environment=self._default_environment))
        487     return self._latest_run_result
        488 


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in run_via_runner_api(self, pipeline_proto)
        492     # TODO(pabloem, BEAM-7514): Create a watermark manager (that has access to
        493     #   the teststream (if any), and all the stages).
    --> 494     return self.run_stages(stage_context, stages)
        495 
        496   @contextlib.contextmanager


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in run_stages(self, stage_context, stages)
        581               stage,
        582               pcoll_buffers,
    --> 583               stage_context.safe_coders)
        584           metrics_by_stage[stage.name] = stage_results.process_bundle.metrics
        585           monitoring_infos_by_stage[stage.name] = (


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in _run_stage(self, worker_handler_factory, pipeline_components, stage, pcoll_buffers, safe_coders)
        902         cache_token_generator=cache_token_generator)
        903 
    --> 904     result, splits = bundle_manager.process_bundle(data_input, data_output)
        905 
        906     def input_for(transform_id, input_id):


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in process_bundle(self, inputs, expected_outputs)
       2103 
       2104     with UnboundedThreadPoolExecutor() as executor:
    -> 2105       for result, split_result in executor.map(execute, part_inputs):
       2106 
       2107         split_result_list += split_result


    /opt/conda/lib/python3.7/concurrent/futures/_base.py in result_iterator()
        596                     # Careful not to keep a reference to the popped future
        597                     if timeout is None:
    --> 598                         yield fs.pop().result()
        599                     else:
        600                         yield fs.pop().result(end_time - time.monotonic())


    /opt/conda/lib/python3.7/concurrent/futures/_base.py in result(self, timeout)
        433                 raise CancelledError()
        434             elif self._state == FINISHED:
    --> 435                 return self.__get_result()
        436             else:
        437                 raise TimeoutError()


    /opt/conda/lib/python3.7/concurrent/futures/_base.py in __get_result(self)
        382     def __get_result(self):
        383         if self._exception:
    --> 384             raise self._exception
        385         else:
        386             return self._result


    /opt/conda/lib/python3.7/site-packages/apache_beam/utils/thread_pool_executor.py in run(self)
         42       # If the future wasn't cancelled, then attempt to execute it.
         43       try:
    ---> 44         self._future.set_result(self._fn(*self._fn_args, **self._fn_kwargs))
         45       except BaseException as exc:
         46         # Even though Python 2 futures library has #set_exection(),


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in execute(part_map)
       2100           self._progress_frequency, self._registered,
       2101           cache_token_generator=self._cache_token_generator)
    -> 2102       return bundle_manager.process_bundle(part_map, expected_outputs)
       2103 
       2104     with UnboundedThreadPoolExecutor() as executor:


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in process_bundle(self, inputs, expected_outputs)
       2023             process_bundle_descriptor_id=self._bundle_descriptor.id,
       2024             cache_tokens=[next(self._cache_token_generator)]))
    -> 2025     result_future = self._worker_handler.control_conn.push(process_bundle_req)
       2026 
       2027     split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in push(self, request)
       1356       self._uid_counter += 1
       1357       request.instruction_id = 'control_%s' % self._uid_counter
    -> 1358     response = self.worker.do_instruction(request)
       1359     return ControlFuture(request.instruction_id, response)
       1360 


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/sdk_worker.py in do_instruction(self, request)
        350       # E.g. if register is set, this will call self.register(request.register))
        351       return getattr(self, request_type)(getattr(request, request_type),
    --> 352                                          request.instruction_id)
        353     else:
        354       raise NotImplementedError


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/sdk_worker.py in process_bundle(self, request, instruction_id)
        384         with self.maybe_profile(instruction_id):
        385           delayed_applications, requests_finalization = (
    --> 386               bundle_processor.process_bundle(instruction_id))
        387           monitoring_infos = bundle_processor.monitoring_infos()
        388           monitoring_infos.extend(self.state_cache_metrics_fn())


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/bundle_processor.py in process_bundle(self, instruction_id)
        815       for op in self.ops.values():
        816         _LOGGER.debug('finish %s', op)
    --> 817         op.finish()
        818 
        819       return ([self.delayed_bundle_application(op, residual)


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.finish()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.finish()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.finish()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.finish()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner._invoke_bundle_method()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner._reraise_augmented()


    /opt/conda/lib/python3.7/site-packages/future/utils/__init__.py in raise_with_traceback(exc, traceback)
        444         if traceback == Ellipsis:
        445             _, _, traceback = sys.exc_info()
    --> 446         raise exc.with_traceback(traceback)
        447 
        448 else:


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner._invoke_bundle_method()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnInvoker.invoke_finish_bundle()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnInvoker.invoke_finish_bundle()


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in finish_bundle(self)
       1035 
       1036   def finish_bundle(self):
    -> 1037     return self._flush_all_batches()
       1038 
       1039   def _flush_all_batches(self):


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in _flush_all_batches(self)
       1042 
       1043     return itertools.chain(*[self._flush_batch(destination)
    -> 1044                              for destination in list(self._rows_buffer.keys())
       1045                              if self._rows_buffer[destination]])
       1046 


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in <listcomp>(.0)
       1043     return itertools.chain(*[self._flush_batch(destination)
       1044                              for destination in list(self._rows_buffer.keys())
    -> 1045                              if self._rows_buffer[destination]])
       1046 
       1047   def _flush_batch(self, destination):


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py in _flush_batch(self, destination)
       1068           rows=rows,
       1069           insert_ids=insert_ids,
    -> 1070           skip_invalid_rows=True)
       1071 
       1072       if not passed:


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery_tools.py in insert_rows(self, project_id, dataset_id, table_id, rows, insert_ids, skip_invalid_rows)
        835     final_rows = []
        836     for i, row in enumerate(rows):
    --> 837       json_row = self._convert_to_json_row(row)
        838       insert_id = str(self.unique_row_id) if not insert_ids else insert_ids[i]
        839       final_rows.append(bigquery.TableDataInsertAllRequest.RowsValueListEntry(


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery_tools.py in _convert_to_json_row(self, row)
        845   def _convert_to_json_row(self, row):
        846     json_object = bigquery.JsonObject()
    --> 847     for k, v in iteritems(row):
        848       if isinstance(v, decimal.Decimal):
        849         # decimal values are converted into string because JSON does not


    /opt/conda/lib/python3.7/site-packages/future/utils/__init__.py in iteritems(obj, **kwargs)
        309     func = getattr(obj, "iteritems", None)
        310     if not func:
    --> 311         func = obj.items
        312     return func(**kwargs)
        313 


    AttributeError: 'str' object has no attribute 'items' [while running 'Cell 15: Write to BQ/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn)']



```python
!pip install --upgrade google-cloud-bigquery
```
