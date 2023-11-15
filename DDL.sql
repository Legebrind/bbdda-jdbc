create table empresa
(
    id        int          not null
        primary key,
    nombre    varchar(255) null,
    direccion varchar(255) null,
    telefono  int          null
)
    charset = utf8mb4;

create table localidad
(
    id        int          not null
        primary key,
    provincia varchar(255) null,
    municipio varchar(255) null,
    nombre    varchar(255) null
);

create table precios
(
    id                       int   not null
        primary key,
    gasolina95E5             float null,
    gasolina95E10            float null,
    gasolina95E5Premium      float null,
    gasolina98E5             float null,
    gasolina98E10            float null,
    gasoleoA                 float null,
    gasoleoPremium           float null,
    gasoleoB                 float null,
    gasoleoC                 float null,
    bioetanol                float null,
    biodiesel                float null,
    gasesLicuadosDelPetroleo float null,
    gasnNaturalComprimido    float null,
    gasNaturalLicuado        float null,
    hidrogeno                float null
)
    charset = utf8mb4;

create table embarcadero
(
    id                 int          not null
        primary key,
    localidadId        int          not null,
    codigoPostal       int          null,
    direccion          varchar(255) null,
    longitud           double       null,
    latitud            double       null,
    preciosId          int          not null,
    gasoleoUsoMaritimo float        null,
    empresaId          int          null,
    tipoVenta          varchar(255) null,
    rem                varchar(2)   null,
    horario            varchar(255) null,
    constraint embarcadero_ibfk_1
        foreign key (localidadId) references localidad (id),
    constraint embarcadero_ibfk_2
        foreign key (preciosId) references precios (id),
    constraint embarcadero_ibfk_3
        foreign key (empresaId) references empresa (id)
);

create index empresaId
    on embarcadero (empresaId);

create index localidadId
    on embarcadero (localidadId);

create index preciosId
    on embarcadero (preciosId);

create table gasolinera
(
    id                int          not null
        primary key,
    localidadId       int          not null,
    codigoPostal      int          null,
    direccion         varchar(255) null,
    margen            varchar(255) null,
    longitud          double       null,
    latitud           double       null,
    tomaDeDatos       date         null,
    preciosId         int          not null,
    porcBioalcohol    float        null,
    porcEsterMetilico float        null,
    empresaId         int          null,
    tipoVenta         varchar(255) null,
    rem               varchar(2)   null,
    horario           varchar(255) null,
    TipoServicio      text         null,
    constraint gasolinera_ibfk_1
        foreign key (localidadId) references localidad (id),
    constraint gasolinera_ibfk_2
        foreign key (preciosId) references precios (id),
    constraint gasolinera_ibfk_3
        foreign key (empresaId) references empresa (id)
);

create index empresaId
    on gasolinera (empresaId);

create index localidadId
    on gasolinera (localidadId);

create index preciosId
    on gasolinera (preciosId);

