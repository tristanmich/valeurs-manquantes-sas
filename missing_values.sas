/* Macro nombre de valeurs manquantes par colonne */
%macro missing_value(Table_path) / minoperator;
	%let payid=%sysfunc(open(&Table_path., is));

	/*Existance de la table et présence ou non d'une lock*/
	%if %sysfunc(exist(&Table_path.)) and &payid. > 0 %then
		%do;
			%let rc=%sysfunc(close(&payid.));

			/*Récupération des infos générales de la table*/
			proc contents data=&Table_path. noprint
				out = WORK.Table_CONTENTS;
			run;

			/*Récupération du nom de la table*/
			%let Table_name = '';

			proc sql noprint;
				select MEMNAME 
					into :Table_name separated by ' '
						from WORK.Table_CONTENTS (obs=1);
			quit;

			/*Récupération du nombre de lignes de la table*/
			proc sql noprint;
				select NOBS
					into :Nb_lignes
						from WORK.Table_CONTENTS (obs=1);
			quit;

			%if &Nb_lignes. ne 0 %then
				%do;
					/*Récupération des informations de l'historique*/
					%let Date_Update=.;
					%let All_Table ='';

					%if %eval(&Date_Update.=.) %then
						%let Date_Update=TODAY();

					/* Récupération du nom des colonnes numériques */
					proc sql;
						create table WORK.Variable_Num as select NAME from WORK.TABLE_Contents where Type=1;
					quit;

					data _NULL_;
						if 0 then
							set WORK.Variable_Num nobs=n;
						call symputx('Nb_Column_num',n);
						stop;
					run;

					/* Récupération du nom des colonnes caractères */
					proc sql;
						create table WORK.Variable_Char as select NAME from WORK.TABLE_Contents where Type=2;
					quit;

					data _NULL_;
						if 0 then
							set WORK.Variable_Char nobs=n;
						call symputx('Nb_Column_char',n);
						stop;
					run;

					/* Boucle car le data step peut traiter 150 colonnes max */
					/* 150 numériques + 150 caractères */
					%let Seuil = 150;
					%let Nb_iteration_num = %sysfunc(ceil(&Nb_Column_num./&Seuil.));
					%let Nb_iteration_char = %sysfunc(ceil(&Nb_Column_char./&Seuil.));

					data WORK.MISSING_RESULT;
					run;

					%do ind=1 %to %sysfunc(max(&Nb_iteration_num.,&Nb_iteration_char.));

						data _NULL_;
							if 0 then
								set WORK.Variable_Num nobs=n;
							call symputx('Nb_Column_num',n);
							stop;
						run;

						data _NULL_;
							if 0 then
								set WORK.Variable_Char nobs=n;
							call symputx('Nb_Column_char',n);
							stop;
						run;

						/* Récupération du nom des colonnes dans une liste */
						proc sql noprint;
							select Name into: Colonne_num separated by ' ' from WORK.Variable_num (obs=&Seuil.);
						quit;

						proc sql noprint;
							select Name into: Colonne_char separated by ' ' from WORK.Variable_char (obs=&Seuil.);
						quit;

						/* Si présence de colonnes numériques et caractères */
						%if &Nb_Column_num. ne 0 and &Nb_Column_char. ne 0 %then
							%do;
								/* data step qui compte le nombre de manquants pour chaque colonne */
								/* Pour le type numérique et caractère */
								data WORK.MISSING_RESULT_TEMP;
									set &Table_path. end = eof;
									N + 1;

									/* Définition des listes */
									array num[*] &Colonne_num.;
									array char[*] &Colonne_char.;

									/*Ne peut pas prendre plus de colonnes que le seuil (environ 150)*/
									array n_count[&Seuil.] _temporary_ (&Seuil.*0);
									array c_count[&Seuil.] _temporary_ (&Seuil.*0);

									do i = 1 to dim(num);
										if missing(num[i]) then
											n_count[i] + 1;
									end;

									do i = 1 to dim(char);
										if missing(char[i]) then
											c_count[i] + 1;
									end;

									if eof then
										do;
											do i = 1 to dim(num);
												Colonne = vname(num[i]);
												Valeur = n_count[i];
												output;
											end;

											do i = 1 to dim(char);
												Colonne = vname(char[i]);
												Valeur = c_count[i];
												output;
											end;
										end;

									keep Colonne N Valeur;
								run;

								/* Ajout des résultats */
								data WORK.MISSING_RESULT;
									set WORK.MISSING_RESULT WORK.MISSING_RESULT_TEMP;
								run;

							%end;

						/* Si absencde de colonnes numériques uniquement */
						%if &Nb_Column_num. ne 0 and &Nb_Column_char. = 0 %then
							%do;

								data WORK.MISSING_RESULT_TEMP;
									set &Table_path. end = eof;
									N + 1;

									/* Définition des listes */
									array num[*] &Colonne_num.;

									/*Ne peut pas prendre plus de colonnes que le seuil (environ 150)*/
									array n_count[&Seuil.] _temporary_ (&Seuil.*0);

									do i = 1 to dim(num);
										if missing(num[i]) then
											n_count[i] + 1;
									end;

									if eof then
										do;
											do i = 1 to dim(num);
												Colonne = vname(num[i]);
												Valeur = n_count[i];
												output;
											end;
										end;

									keep Colonne N Valeur;
								run;

								/* Ajout des résultats */
								data WORK.MISSING_RESULT;
									set WORK.MISSING_RESULT WORK.MISSING_RESULT_TEMP;
								run;

							%end;

						/* Si absencde de colonnes caractères uniquement */
						%if &Nb_Column_num. = 0 and &Nb_Column_char. ne 0 %then
							%do;

								data WORK.MISSING_RESULT_TEMP;
									set &Table_path. end = eof;
									N + 1;

									/* Définition des listes */
									array char[*] &Colonne_char.;

									/*Ne peut pas prendre plus de colonnes que le seuil (environ 150)*/
									array c_count[&Seuil.] _temporary_ (&Seuil.*0);

									do i = 1 to dim(char);
										if missing(char[i]) then
											c_count[i] + 1;
									end;

									if eof then
										do;
											do i = 1 to dim(char);
												Colonne = vname(char[i]);
												Valeur = c_count[i];
												output;
											end;
										end;

									keep Colonne N Valeur;
								run;

								/* Ajout des résultats */
								data WORK.MISSING_RESULT;
									set WORK.MISSING_RESULT WORK.MISSING_RESULT_TEMP;
								run;

							%end;

						/* Suppresion des colonnnes deja traitées */
						data WORK.Variable_Num;
							set WORK.Variable_Num;

							if _n_ <= &Seuil. then
								delete;
						run;

						data WORK.Variable_Char;
							set WORK.Variable_Char;

							if _n_ <= &Seuil. then
								delete;
						run;

					%end;

					/* Suppression des valeurs manquantes */
					data WORK.MISSING_RESULT;
						set WORK.MISSING_RESULT;
						where Colonne is not missing;
					run;

					/*Jointure sur les colonnes de la table pour ne pas oublier de valeurs*/
					PROC SQL;
						CREATE TABLE WORK.MISSING_VALUE_JOIN AS 
							SELECT DISTINCT t1.Name as Colonne, 
								t2.Valeur
							FROM WORK.TABLE_CONTENTS t1
								LEFT JOIN WORK.MISSING_RESULT t2 
									ON (t1.NAME = t2.Colonne);
					QUIT;

					/*Si absent donc nombre de manquant = 0*/
					data WORK.MISSING_VALUE_JOIN;
						set WORK.MISSING_VALUE_JOIN;
						format Valeur;
						array change _numeric_;

						do over change;
							if change=. then
								change=0;
						end;
					run;

					%let table = %scan(&Table_path.,2,'.');

					%if %scan(&Table_path., 1, '.')=INPUT %then
						%let Type_Table = Input;
					%else %let Type_Table = Dataset;

					/*Formatage des données*/
					PROC SQL;
						CREATE TABLE WORK.MISSING_FORMAT AS 
							SELECT  ("&Type_table.") as Type length 7,
								("&Table.") AS Table length 32, 
								('Valeurs manquantes') AS Type_Controle, 
								t1.Colonne length=32, 
								t1.Valeur,
								(1) as Criticite, TODAY() AS Date format = DDMMYY10., 
								&Date_Update. AS Previous_Date format = DDMMYY10., 
								('Colonne') as Info length 7 
							FROM WORK.MISSING_VALUE_JOIN t1;
					QUIT;

					data WORK.TODAY_MISSING_VALUE_Tmp;
					run;

                    PROC SQL;
                        CREATE TABLE WORK.TODAY_MISSING_VALUE_Temp AS 
                            SELECT t1.Type, 
                                t1.Table, 
                                t1.Type_Controle, 
                                t1.Colonne AS Colonne, 
                                t1.Valeur,
                                (0) AS Previous_Valeur,
                                (0) AS Delta, 
                                t1.Criticite,
                                t1.Date, 
                                t1.Previous_Date,
                                t1.Info
                            FROM WORK.MISSING_FORMAT t1;
                    QUIT;

                    data WORK.TODAY_MISSING_VALUE_Tmp;
                        set WORK.TODAY_MISSING_VALUE_Temp;
                        array change Delta;

                        do over change;
                            if change=. then
                                change=0;
                        end;
                    run;

					data WORK.TODAY_MISSING_VALUE_Tmp;
						set WORK.TODAY_MISSING_VALUE_Tmp;
						where Type is not missing;
					run;

				%end;

			/*Suppression des tables*/
			proc datasets lib = WORK noprint;
				delete TABLE_CONTENTS VARIABLE_NUM VARIABLE_CHAR MISSING_RESULT
					MISSING_RESULT_TEMP MISSING_VALUE_JOIN MISSING_FORMAT
					TODAY_MISSING_VALUE_TEMP;
			run;

		%end;
%mend Missing_value;