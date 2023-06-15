package org.example;

import java.util.ArrayList;
import java.util.List;

public class rechercheJavaOracleNoSQL {
    //Rechercher toutes les notes attribuées à un participant sur un tournoi (calcul du score) :

    public List<Double> getParticipantScores(int participantId, int tournoisId) {
        List<Double> scores = new ArrayList<>();

        // Code pour accéder à la base de données Oracle NoSQL et récupérer les notes attribuées au participant sur le tournoi

        return scores;
    }

//Rechercher toutes les notes attribuées par un juge lors d'un tournoi (vérifier si le juge ne met pas de trop haute/basses notes) :

    public List<Double> getJudgeNotes(int jugeId, int tournoisId) {
        List<Double> notes = new ArrayList<>();

        // Requête SQL pour récupérer les notes attribuées par le juge lors du tournoi
        // SELECT note FROM note WHERE id_j = jugeId AND id_t = tournoisId;

        // Exécution de la requête et récupération des résultats dans une liste notes

        return notes;
    }

//Rechercher tous les scores de tous les participants à un tournoi (calcul classement) :

    public List<Double> getAllParticipantScores(int tournoisId) {
        List<Double> scores = new ArrayList<>();

        // Requête SQL pour récupérer les scores de tous les participants au tournoi
        // SELECT score FROM participant WHERE id_t = tournoisId;

        // Exécution de la requête et récupération des résultats dans une liste scores

        return scores;
    }

// Rechercher tous les participants d'un tournoi (préparation du classement, liste de participation, ...) :

    public List<String> getParticipants(int tournoisId) {
        List<String> participants = new ArrayList<>();

        // Requête SQL pour récupérer les participants du tournoi
        // SELECT nom_i, prenom_i FROM inscrit WHERE id_t = tournoisId;

        // Exécution de la requête et récupération des résultats dans une liste participants

        return participants;
    }

//Rechercher tous les juges d'un tournoi (liste de participation) :

    public List<String> getJuges(int tournoisId) {
        List<String> juges = new ArrayList<>();

        // Requête SQL pour récupérer les juges du tournoi
        // SELECT nom_i, prenom_i FROM juge INNER JOIN inscrit ON juge.id_inscrit = inscrit.id_inscrit WHERE id_t = tournoisId;

        // Exécution de la requête et récupération des résultats dans une liste juges

        return juges;
    }

//Rechercher tous les organisateurs de tournoi (vérifications) :

    public List<String> getOrganisateurs() {
        List<String> organisateurs = new ArrayList<>();

        // Requête SQL pour récupérer tous les organisateurs de tournoi
        // SELECT DISTINCT organisateur_t FROM tournois;

        // Exécution de la requête et récupération des résultats dans une liste organisateurs

        return organisateurs;
    }
}
