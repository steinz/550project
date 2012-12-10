
CONFIG:=static

all:
	@rm -f config.json
	@ln -s config-$(CONFIG).json config.json

clean:
	@rm -rf *.pyc *~ config.json
