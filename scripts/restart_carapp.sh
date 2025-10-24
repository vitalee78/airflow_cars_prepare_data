#!/bin/bash

# Перезапуск carapp.service
sudo systemctl restart carapp

if systemctl is-active --quiet carapp; then
    echo "$(date): carapp.service успешно перезапущен."
else
    echo "$(date): ОШИБКА: carapp.service не запущен после перезапуска!" >&2
    exit 1
fi