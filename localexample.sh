sbt package
spark-submit --class "LocalExample" --master local[4] target/scala-2.12/hello-world_2.12-1.0.jar