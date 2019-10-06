# telegram-bot-media-converter


## Config

### Env file

Create a `.env` file with a `TELEGRAM_TOKEN` variable.

### Webhook

To configure the webhook open the terminal and run:
```
curl --request POST --url https://api.telegram.org/bot{{telegram_token}}/setWebhook --header 'content-type: application/json' --data '{"url": "{{your_set_webhook_url}}"}'
```