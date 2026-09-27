# RESQ-AI

Prototype généraliste de coordination multi-crises. Chaque couple type de crise / localisation possède ses propres signalements, établissements, recommandations et simulations. L'inondation à Casablanca reste uniquement le jeu de données initial de démonstration.

## Démarrage

### Base PostgreSQL

Le backend se connecte à PostgreSQL avec `root` / `root` sur `localhost:5432`. Au démarrage, il crée automatiquement la base `resq_ai` si elle manque (le rôle PostgreSQL doit avoir le droit `CREATEDB`), puis les tables et les données de démonstration. Une installation PostgreSQL Windows déjà active sur le port 5432 suffit; dans ce cas, ne démarrez pas aussi le conteneur Docker.

Si PostgreSQL n'est pas installé localement, démarrez Docker Desktop puis, depuis la racine du projet :

```powershell
docker compose up -d
```

Le conteneur utilise aussi la base `resq_ai` et `root` / `root`. Le port local 5432 doit être libre. Ce mot de passe est réservé au prototype local. Le volume Docker `resq_postgres_data` conserve les données quand le conteneur s'arrête.

### Backend

```powershell
cd backend
uv venv .venv314 --python 3.14.7
uv pip install --python .venv314\Scripts\python.exe -r requirements.txt
.\.venv314\Scripts\python.exe -m uvicorn app.main:app --reload --env-file .env
```

API et documentation interactive : http://localhost:8000/docs

### Frontend

```powershell
cd frontend
npm install
npm run dev
```

L'application utilise des données de démonstration fictives. Les recommandations sont une aide à la décision, pas des ordres opérationnels. Un responsable doit les valider.

## Stockage et accès

Au premier démarrage du backend, les tables sont créées depuis [`backend/db/schema.sql`](backend/db/schema.sql) et des données fictives sont insérées dans PostgreSQL : inondation à Casablanca, séisme à Rabat et incendie à Lyon. Un profil opérateur local est fourni pour le prototype. Ce mode ne convient pas à un déploiement public multi-utilisateur : il n'y a pas d'authentification ni de contrôle d'accès par compte. Le fichier SQLite utilisé précédemment est laissé intact, mais n'est plus lu par l'application.

## Fonctionnalités disponibles

- Contexte de crise configurable (inondation, séisme, incendie, tempête, glissement de terrain, accident industriel, épidémie ou autre) et localisation libre.
- Plusieurs crises conservées indépendamment dans PostgreSQL ; revenir au même type/lieu restaure son historique.
- Tableau de situation, incidents et carte interactive.
- Calcul d’itinéraires entre un signalement et un hôpital, avec alternatives, détails des routes et durée/distance estimées. Les signalements de blocage et de danger sont projetés sur la carte.
- À l’application d’une région, recherche et enregistrement automatiques des hôpitaux et stations d’ambulance cartographiés dans un rayon de 20 km. Les résultats OSM sont mis en cache et dédoublonnés par identifiant cartographique.
- Accès local sans compte, pour démonstration sur une machine de confiance.
- Sauvegarde automatique dans PostgreSQL des signalements, scénarios, hôpitaux, ambulances et simulations.
- Création et qualification de signalements citoyens avec coordonnées GPS mondiales.
- Les hôpitaux et stations trouvés sur OpenStreetMap sont rattachés automatiquement à la crise sélectionnée et placés sur la carte. Les ajouts manuels restent disponibles pour compléter la carte.
- Simulation qualitative avec chaînes de conséquences adaptées au type de crise.
- Plan d'action priorisé soumis à validation humaine.
- Six agents : **Crisis Commander** (synthèse centrale), **Disaster Detection** (photos citoyennes et carte des signalements), **Rescue** (priorités, routes et besoin indicatif d’ambulances), **Medical** (lits, charge et stocks déclarés), **Epidemic** (motifs de symptômes déclarés, sans diagnostic) et **Simulation** (branches qualitatives). Ollama génère leurs analyses si disponible; sinon les règles locales produisent les résultats.
- Scénario d’exercice complet isolé **Séisme — Marrakech** : six signalements, trois hôpitaux fictifs, quatre ambulances, un message vocal de démonstration, une illustration synthétique pour tester la vision et une simulation enregistrée. Le bouton **Charger le scénario test complet** l’active sans remplacer les autres crises.
- Signalements citoyens avec photo (JPEG/PNG/WebP, 2,4 Mo maximum) et dictée/transcription vocale du navigateur, conservés avec l’incident.
- Répertoire d’experts avec profils de démonstration explicitement fictifs et canal de contact optionnel pour les profils réellement ajoutés.
- Simulateur comparant trois branches qualitatives (amélioration, centrale, aggravation) à partir des données déclarées.

Les hôpitaux, incidents initiaux, profils experts initiaux et capacités du scénario de démonstration sont fictifs. Pour la recherche de ressources, le backend géocode la région quand l’opérateur clique sur **Appliquer**, puis interroge Nominatim et Overpass d’OpenStreetMap. Le géocodage est mis en cache 30 jours et les résultats de ressources 24 heures. Les stations d’ambulance cartographiées sont des emplacements, pas des véhicules suivis : disponibilité, effectif et lits restent inconnus tant qu’un opérateur ne les confirme pas. Les cartes utilisent les données des contributeurs OpenStreetMap sous licence ODbL; les services publics peuvent être temporairement indisponibles. Le calcul OSRM ne dispose pas du trafic ni des fermetures en temps réel. Aucune imagerie satellite/drone ni analyse de dommages par segmentation n’est connectée : seules les photos ajoutées à un signalement sont éventuellement analysées par le modèle local. Les durées d’itinéraire, capacités médicales, profils et disponibilités doivent être confirmés par des personnes qualifiées. Les agents utilisent des règles locales ou Ollama; la vision dépend du modèle. Aucune action de secours n'est exécutée automatiquement. Les sorties sont des aides à la décision, non des faits vérifiés ni des consignes de secours.

### Activer le modèle IA local (optionnel)

Téléchargez et installez [Ollama pour Windows](https://ollama.com/download/windows). Ollama lance normalement son API locale en arrière-plan sur `http://localhost:11434`. Dans PowerShell, téléchargez le modèle :

```powershell
ollama pull gemma3:1b
```

L'interface devrait ensuite indiquer **IA générative active**. Cliquez sur **Exécuter les agents IA** pour lancer les agents spécialistes, puis le Crisis Commander qui synthétise leurs résultats. Les recommandations médicales, le niveau de danger visuel, les fermetures et les disponibilités nécessitent toujours une confirmation humaine. Vérifiez l'installation avec `ollama list`. Si l'API Ollama n'est pas démarrée, ouvrez l'application Ollama ou lancez `ollama serve`. Sans modèle disponible, les agents utilisent le moteur de règles et l'interface l'indique.

## API principale

- `GET /api/dashboard`, `/api/incidents`, `/api/hospitals`, `/api/scenarios`
- `POST /api/incidents` (champs optionnels `photo_data` et `voice_transcript`)
- `GET /api/experts`, `POST /api/experts` (répertoire d’experts)
- `POST /api/routes/plan` (itinéraires alternatifs entre un signalement et un hôpital)
- `POST /api/hospitals`
- `PATCH /api/incidents/{id}/validation`, `/api/incidents/{id}/resolve`
- `PUT /api/scenarios/active`
- `POST /api/resources/discover` (cherche et rattache à la crise active les hôpitaux et stations cartographiés autour du lieu)
- `POST /api/simulation`
- `GET /api/agents/status`, `POST /api/agents/run`
