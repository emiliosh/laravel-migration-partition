FROM php:8.1-fpm

RUN apt-get update && apt-get install -y \
    curl \
    git \
    unzip \
    zip

# Get latest Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer
