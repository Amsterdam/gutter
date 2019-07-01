FROM debian:jessie

# docker file based on https://github.com/ZHAJOR/Docker-Postgresql-9.4-Phppgadmin and https://github.com/jacksoncage$
# some chances by MN

RUN apt-get update
RUN apt-get -y install apache2 libapache2-mod-php5 php5 php5-pgsql vim wget unzip
RUN apt-get clean

ENV APACHE_RUN_USER www-data
ENV APACHE_RUN_GROUP www-data
ENV APACHE_LOG_DIR /var/log/apache2

RUN ln -sf /dev/stdout /var/log/apache2/access.log
RUN ln -sf /dev/stdout /var/log/apache2/error.log

RUN chown -R www-data:www-data /var/log/apache2 /var/www/html

WORKDIR /var/www/html
RUN wget https://github.com/phppgadmin/phppgadmin/archive/master.zip
RUN rm /var/www/html/index.html && unzip /var/www/html/master.zip
RUN cp -R phppgadmin-master/* . && rm -r phppgadmin-master

#RUN cp conf/config.inc.php-dist conf/config.inc.php
#RUN sed -i "s/\$conf\['extra_login_security'\] = true;/\$conf\['extra_login_security'\] = false;/g" conf/config.inc$
#RUN sed -i "s/\$conf\['servers'\]\[0\]\['host'\] = '';/\$conf\['servers'\]\[0\]\['host'\] = 'localhost';/g" conf/co$
#RUN sed -i "s/\$conf\['servers'\]\[0\]\['port'\] = 5432;/\$conf\['servers'\]\[0\]\['port'\] = 5435;/g" conf/config.$

ADD ./config.inc.php conf/config.inc.php
RUN sed -i 's/variables_order = "GPCS"/variables_order = "EGPCS"/g' /etc/php5/apache2/php.ini # needed to import env$

# env variables
ENV POSTGRES_DEFAULTDB defaultdb
ENV POSTGRES_HOSTNAME 'default hostname'
ENV POSTGRES_HOST localhost
ENV POSTGRES_PORT 5432

EXPOSE 80

CMD ["rm", "-f", "/run/apache2/apache2.pid"]
CMD ["/usr/sbin/apache2ctl", "-D", "FOREGROUND"]