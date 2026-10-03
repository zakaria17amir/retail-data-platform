# Windows setup

The platform is developed on Windows with Docker Desktop (WSL2 backend). Scripts run inside
containers or Git Bash.

## 1. Docker Desktop

Install Docker Desktop and enable **Use the WSL 2 based engine** (Settings → General).

## 2. Give WSL2 enough resources

```sh
cp scripts/wslconfig.example "$USERPROFILE/.wslconfig"
wsl --shutdown
```

Restart Docker Desktop afterwards. Adjust `memory` and `processors` to your machine.

## 3. Ollama

Install Ollama natively on Windows (not in a container) and pull the models named in the spec.
Containers reach it at `http://host.docker.internal:11434`.

## 4. Git line endings

Set `git config --global core.autocrlf false`. The repo's `.gitattributes` enforces LF.

## 5. Kaggle credentials

On kaggle.com open Settings → API and create a token. Put the values in `.env`:

```
KAGGLE_USERNAME=<your username>
KAGGLE_KEY=<your key>
```
