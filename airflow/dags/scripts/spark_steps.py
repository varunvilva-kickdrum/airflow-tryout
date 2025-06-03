SPARK_STEPS = [
    {
        'Name': 'Process CSV with Hudi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', '/usr/lib/hudi/hudi-spark3-bundle_2.12-0.15.0-amzn-6.jar',
                '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
                's3://varun-demo-temp-s3-vilva/scripts/transform_to_hudi.py',
            ],
        },
    }
]