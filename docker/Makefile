all:

#image:
#	docker build -t gitlab.ugrid.net:5000/ugrid-all:latest .
#
#run:
#	docker run --rm --name ugrid-all -ti -p 50070:50070 -p 8000:8000 -p 8080:8080 -p 12346:12346 -p 12348:12348 -e UGRID_SERVER=192.168.59.103 gitlab.ugrid.net:5000/ugrid-all:latest

rmi:
	docker rmi gitlab.ugrid.net:5000/ugrid:latest

image:
	docker build -t gitlab.ugrid.net:5000/ugrid:latest .

run:
	docker run --rm --name ugrid -ti -p 2200:22 -p 8000:8000 -p 12346:12346 -p 12348:12348 -e UGRID_SERVER=192.168.99.100 gitlab.ugrid.net:5000/ugrid:latest

publish:
	docker push gitlab.ugrid.net:5000/ugrid:latest
