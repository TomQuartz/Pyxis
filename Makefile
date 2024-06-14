all: netbricks
	(cd db; cargo build --release)
	(cd compute; cargo build --release)
	(cd ext/pushback; cargo build --release)
	(cd ext/vector; cargo build --release)

netbricks:
	(cd net/native; make)
	mkdir -p net/target/native
	cp net/native/libzcsi.so net/target/native/libzcsi.so

clean:
	(cd db; cargo clean)
	(cd compute; cargo clean)
	(cd ext/pushback; cargo clean)
	(cd ext/vector; cargo clean)
	(cd sandstorm; cargo clean)
	(cd net; ./build.sh clean)
	(cd util; cargo clean)
