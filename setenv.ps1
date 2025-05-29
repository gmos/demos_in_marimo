# Convince UV to create/find the VENV in my tmp folder
$env:VIRTUAL_ENV = "$HOME\tmp\demo_venv"
$env:UV_PROJECT_ENVIRONMENT = "$HOME\tmp\demo_venv"

# And activate it
& "$HOME\tmp\demo_venv\Scripts\activate.ps1"

