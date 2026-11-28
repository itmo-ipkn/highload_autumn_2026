.PHONY: up verify test unit status stop down reset
up:
	./local-compose up -d --build
verify:
	./local-compose run --rm --no-deps verify
test:
	./local-compose run --rm --no-deps tests
unit:
	python3 -m unittest discover -s tests -v
status:
	./status
stop:
	./local-compose stop
down:
	./local-compose down
# Explicitly deletes only this project's four volumes. Never used by smoke.sh.
reset:
	./local-compose down --volumes
