docker run -v /home/gianglt2/project/big-data:/home/gianglt2/project/big-data \
           --name spark \
           -w /home/gianglt2/project/big-data \
           -i -t -p 8888:8888 --rm spark
