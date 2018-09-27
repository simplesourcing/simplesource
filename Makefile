build:
	mvn install

deploy:
	openssl aes-256-cbc -K ${encrypted_bbcf2f683b6c_key} -iv ${encrypted_bbcf2f683b6c_iv} -in .deploy/private-key.gpg.enc -out .deploy/private-key.gpg -d
	gpg --import .deploy/private-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -DskipTests -P release --settings .deploy/settings.xml
