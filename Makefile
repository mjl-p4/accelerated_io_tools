all:
	$(MAKE) -C src
	@cp src/*.so .

test:
	./tests/test.sh

clean:
	$(MAKE) -C src clean
	rm -f *.so
