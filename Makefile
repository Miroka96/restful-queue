run: build start

build:
	docker build -t restful-queue .

start:
	docker run -p 127.0.0.1:8765:8080 --name restful-queue --link mysql:mysql -e HOST=mysql -e DATABASE=queue -e USER=queue -e PASSWORD=secret --restart always -d restful-queue
