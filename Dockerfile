FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY eshop_crawler.py ./

CMD [ "python", "./eshop_crawler.py" ]