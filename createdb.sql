

create database ResultTracker;
use ResultTracker;

create table Players (player_id int(16) primary key not null, tag varchar(20) not null, real_name varchar(35) not null, nationality varchar(20) not null, birthday Date not null, game_race varchar(4) not null);
create table Teams (team_id int(12) not null primary key, name varchar(35) not null, founded Date not null, disbanded Date);
create table Members (player int(16) not null, team int(8) not null, start_date Date not null, end_date Date, primary key (player,start_date), constraint foreign key(player) references players (player_id), constraint foreign key (team) references teams(team_id));
create table tournaments (tournament_id int(16) primary key not null, name varchar(100) not null, region varchar(16), major tinyint(1) not null);
create table matches (match_id int(20) primary key not null, date Date not null, tournament int(16) not null, playerA int(16) not null, playerB int(16) not null, scoreA int(4) not null, scoreB int(4) not null, offline tinyint(1) not null, constraint foreign key (tournament) references tournaments(tournament_id), constraint foreign key (playerA) references players(player_id), constraint foreign key (playerB) references players(player_id));
create table earnings (tournament int(16) not null, player int(16) not null, prize_money int(16) not null, position int(8) not null, primary key (tournament,player), constraint foreign key(player) references players (player_id), constraint foreign key (tournament) references tournaments(tournament_id));
