@echo off
call sbt package
call spark-submit --class "SimpleApp" --master local[*] target/scala-2.12/hello-world_2.12-1.0.jar %1 %2