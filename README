# attribution

forked from: https://github.com/saloedov/nintendo_switch_deals_crawler

email code from: https://realpython.com/python-send-email/#sending-your-plain-text-email

dockerized using python template from: https://hub.docker.com/_/python

# building

    docker build -t nintendo_switch_deals_notifier .

# running

setup an `.env` file like this:

    NOTIFICATION_SMTP_SERVER=someserver
    NOTIFICATION_SENDER=a@b.c
    NOTIFICATION_RECEIVER=a@b.c,a@b.c
    NOTIFICATION_SMTP_PASSWORD=abc
    GAMES=My Time at Portia,Super Mario Maker 2,7 Billion Humans

then run:

    docker run -it --env-file ./.env --rm nintendo_switch_deals_notifier