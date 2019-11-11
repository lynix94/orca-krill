FROM lynix94/orca-lang:latest
MAINTAINER lynix94

RUN git clone https://github.com/lynix94/orca-krill

WORKDIR /orca-krill
EXPOSE 6379
CMD ["orca", "run.orca"]

