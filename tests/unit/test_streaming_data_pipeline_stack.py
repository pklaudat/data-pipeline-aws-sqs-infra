import aws_cdk as core
import aws_cdk.assertions as assertions

from streaming_data_pipeline.streaming_data_pipeline_stack import StreamingDataPipelineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in streaming_data_pipeline/streaming_data_pipeline_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = StreamingDataPipelineStack(app, "streaming-data-pipeline")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
