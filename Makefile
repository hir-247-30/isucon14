SHELL := /bin/bash
APP_NAME := isuride

# nginxのログ解析alpコマンド
alp:
	sudo cat /var/log/nginx/access.log | alp ltsv --sort sum -r \
                                    -m "/api/chair/rides/.*/status,/api/app/rides/.*/evaluation,/@\w+t,/image/\d+" \
                                    -o count,method,uri,min,avg,max,sum

clear:
	sudo rm /var/log/mysql/mysql-slow.log && \
	sudo rm /var/log/nginx/access.log && \
	sudo systemctl restart mysql && \
	sudo systemctl restart nginx
