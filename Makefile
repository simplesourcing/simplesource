build:
	mvn install

deploy:
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -DskipTests -P release --settings .deploy/settings.xml
