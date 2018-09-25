build:
	mvn install

deploy:
	gpg --import .deploy/private-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -DskipTests -P release --settings .deploy/settings.xml
