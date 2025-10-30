python -m venv .venv
.venv\Scripts\activate

.venv\Scripts\python.exe -m pip install --upgrade pip 

pip install numpy pandas scikit-learn matplotlib seaborn

pip freeze > requeriments.txt

pip install -r requeriments.txt

Para trabajar con notebook
.venv\Scripts\activate
pip install jupyter ipykernel
python -m ipykernel install --user --name=env_inpec --display-name "Inpec env"