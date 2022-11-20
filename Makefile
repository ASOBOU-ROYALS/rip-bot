run_prod:
	gunicorn --certfile=/etc/letsencrypt/live/discord.rip-bot.com/fullchain.pem --keyfile=/etc/letsencrypt/live/discord.rip-bot.com/privkey.pem --bind 0.0.0.0:443 wsgi:app
