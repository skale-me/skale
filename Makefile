all:

# When editing docs, run 'make edit' then browse http://localhost:8000
edit:
	docker run --rm -it -p 8000:8000 -v $$HOME/.ssh:/root/.ssh -v $${PWD%/*}:/docs squidfunk/mkdocs-material

build:
	docker run --rm -it -v $${PWD%/*}:/docs squidfunk/mkdocs-material build

# Maintainer only, to deploy static doc website on github pages
deploy:
	docker run --rm -it -v $HOME/.ssh:/root/.ssh -v $${PWD%/*}:/docs squidfunk/mkdocs-material gh-deploy
