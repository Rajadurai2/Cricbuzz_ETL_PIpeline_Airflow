echo "Copying files from local to Hadoop Docker image..."

hdfs dfs -ls

docker exec -it namenode hdfs dfs -mkdir /Cricbuzz
docker exec -it namenode hdfs dfs -mkdir /Cricbuzz/Cricbuzz_files
docker exec -it namenode hdfs dfs -mkdir /Cricbuzz/Cricbuzz_files/Scraped_raw_files
docker exec -it namenode hdfs dfs -mkdir /Cricbuzz/Cricbuzz_files/Cleaned_files


docker exec -it namenode hdfs dfs -put -f /Scraped__raw_files /Cricbuzz/Cricbuzz_files/Scraped_raw_files
docker exec -it namenode hdfs dfs -put -f /Transformed_files /Cricbuzz/Cricbuzz_files/Transformed_files

rm Scraped_raw_files/*
rm Transformed_files/*
