{#
    Fenêtre glissante de 8 exercices d'immatriculation (décision du
    2026-08-29) : plutôt que de coder en dur "2018-2025" dans chaque modèle
    (ce qui figerait le pipeline à cette période pour toujours), on calcule
    les 8 années cibles à l'exécution -- année_courante - 8 à année_courante
    - 1 (le dernier exercice complet est toujours l'année précédente, pas
    l'année en cours). Ex. en 2026 : 2018-2025. En 2027 : 2019-2026.

    Robustesse voulue vis-à-vis de la source (SDES) : le fichier réel
    contient aujourd'hui un historique complet 2010-2025 (confirmé le
    2026-08-29 sur les fichiers réels), pas déjà une fenêtre de 8 ans -- on
    ne sait pas si la source elle-même glissera un jour sa propre fenêtre.
    Peu importe : ce calcul ne dépend QUE de la date d'exécution, jamais du
    nombre de colonnes présentes dans le fichier source. `postgres_loader.py`
    applique le même calcul côté Python (voir sa docstring) pour ne charger
    en base que les colonnes de cette fenêtre, quelle que soit la largeur du
    fichier reçu.

    `run_started_at` (timestamp UTC fourni par dbt pour tout le run) plutôt
    que `modules.datetime.datetime.now()` : une seule valeur pour tous les
    modèles d'un même run, même si l'exécution dbt franchit minuit ou le 1er
    janvier pendant qu'elle tourne.

    Retourne une liste Python de 8 entiers, du plus ancien au plus récent
    (ex. [2018, 2019, ..., 2025]) -- utilisable telle quelle avec `[:3]`
    (3 plus anciennes, "baseline") et `[-3:]` (3 plus récentes, "recent")
    dans les modèles appelants (int_territorial_join.sql).
#}
{% macro immat_years() %}
    {% set annee_recente = run_started_at.year - 1 %}
    {% set annee_ancienne = annee_recente - 7 %}
    {{ return(range(annee_ancienne, annee_recente + 1) | list) }}
{% endmacro %}
