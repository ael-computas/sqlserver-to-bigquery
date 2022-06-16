FROM python:3.7-buster
# add microsoft keys to apt
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Debian 10
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update; apt-get upgrade -y; ACCEPT_EULA=Y apt-get install -y msodbcsql17; apt-get install -y unixodbc-dev

RUN pip3 install --upgrade pip
COPY requirements.txt /program/requirements.txt
RUN pip3 install -r /program/requirements.txt
COPY . /program
WORKDIR /program
EXPOSE 8080
ENV PORT 8080
CMD python main.py