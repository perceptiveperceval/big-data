docker run -v /home/hadoop/project/big-data:/home/hadoop/project/big-data \
           --name spark \
           -w /home/hadoop/project/big-data \
           -i -t -p 8888:8888 --rm spark
