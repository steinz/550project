
CONFIG:=2ring-dynamic

all:
	@rm -f config.json
	@ln -s config-$(CONFIG).json config.json

clean:
	@rm -rf *.pyc *~ config.json

lines:
	wc -l *.py | grep total
