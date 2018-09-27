build:
	mvn install

deploy:
	openssl aes-256-cbc -K ${encrypted_bbcf2f683b6c_key} -iv ${encrypted_bbcf2f683b6c_iv} -in .deploy/private-key.gpg.enc -out .deploy/private-key.gpg -d
	gpg --import .deploy/private-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -DskipTests -P release --settings .deploy/settings.xml

docs:
	mvn javadoc:javadoc -P release
	git config --global user.email "travis@travis-ci.com"
	git config --global user.name "travis-ci"
	rm -rf simplesourcing.github.io
	git clone --quiet https://${GITHUB_TOKEN}@github.com/simplesourcing/simplesourcing.github.io > /dev/null
	cd simplesourcing.github.io && \
	    rm -rf apidocs && \
	    cp -Rf ../target/site/apidocs apidocs && \
	    git add . && \
        git commit -m "Lastest javadoc on successful travis deploy ${TRAVIS_BUILD_NUMBER} auto-pushed to simplesourcing.github.io" && \
	    git push -fq > /dev/null
	rm -rf simplesourcing.github.io
