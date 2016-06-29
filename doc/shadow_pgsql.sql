--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: hstore; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;


--
-- Name: EXTENSION hstore; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION hstore IS 'data type for storing sets of (key, value) pairs';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: array_types; Type: TABLE; Schema: public; Owner: zilence; Tablespace: 
--

CREATE TABLE array_types (
    id integer NOT NULL,
    aint2 smallint[],
    aint4 integer[],
    aint8 bigint[],
    atext text[],
    anumeric numeric[]
);


ALTER TABLE public.array_types OWNER TO zilence;

--
-- Name: array_types_id_seq; Type: SEQUENCE; Schema: public; Owner: zilence
--

CREATE SEQUENCE array_types_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.array_types_id_seq OWNER TO zilence;

--
-- Name: array_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: zilence
--

ALTER SEQUENCE array_types_id_seq OWNED BY array_types.id;


--
-- Name: binary_types; Type: TABLE; Schema: public; Owner: zilence; Tablespace: 
--

CREATE TABLE binary_types (
    id integer NOT NULL,
    fbytea bytea
);


ALTER TABLE public.binary_types OWNER TO zilence;

--
-- Name: binary_types_id_seq; Type: SEQUENCE; Schema: public; Owner: zilence
--

CREATE SEQUENCE binary_types_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.binary_types_id_seq OWNER TO zilence;

--
-- Name: binary_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: zilence
--

ALTER SEQUENCE binary_types_id_seq OWNED BY binary_types.id;


--
-- Name: dummy; Type: TABLE; Schema: public; Owner: zilence; Tablespace: 
--

CREATE TABLE dummy (
    nnull text NOT NULL
);


ALTER TABLE public.dummy OWNER TO zilence;

--
-- Name: num_types; Type: TABLE; Schema: public; Owner: zilence; Tablespace: 
--

CREATE TABLE num_types (
    id integer NOT NULL,
    fint2 smallint,
    fint4 integer,
    fint8 bigint,
    fnumeric numeric
);


ALTER TABLE public.num_types OWNER TO zilence;

--
-- Name: num_types_id_seq; Type: SEQUENCE; Schema: public; Owner: zilence
--

CREATE SEQUENCE num_types_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.num_types_id_seq OWNER TO zilence;

--
-- Name: num_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: zilence
--

ALTER SEQUENCE num_types_id_seq OWNED BY num_types.id;


--
-- Name: timestamp_types; Type: TABLE; Schema: public; Owner: zilence; Tablespace: 
--

CREATE TABLE timestamp_types (
    id integer NOT NULL,
    ftimestamp timestamp without time zone,
    ftimestamptz timestamp with time zone,
    fdate date
);


ALTER TABLE public.timestamp_types OWNER TO zilence;

--
-- Name: timestamp_types_id_seq; Type: SEQUENCE; Schema: public; Owner: zilence
--

CREATE SEQUENCE timestamp_types_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.timestamp_types_id_seq OWNER TO zilence;

--
-- Name: timestamp_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: zilence
--

ALTER SEQUENCE timestamp_types_id_seq OWNED BY timestamp_types.id;


--
-- Name: types; Type: TABLE; Schema: public; Owner: zilence; Tablespace: 
--

CREATE TABLE types (
    id integer NOT NULL,
    t_bool boolean,
    t_int2 smallint,
    t_int4 integer,
    t_int8 bigint,
    t_float4 real,
    t_float8 double precision,
    t_text text,
    t_hstore hstore,
    t_uuid uuid
);


ALTER TABLE public.types OWNER TO zilence;

--
-- Name: types_id_seq; Type: SEQUENCE; Schema: public; Owner: zilence
--

CREATE SEQUENCE types_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.types_id_seq OWNER TO zilence;

--
-- Name: types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: zilence
--

ALTER SEQUENCE types_id_seq OWNED BY types.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: zilence
--

ALTER TABLE ONLY array_types ALTER COLUMN id SET DEFAULT nextval('array_types_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: zilence
--

ALTER TABLE ONLY binary_types ALTER COLUMN id SET DEFAULT nextval('binary_types_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: zilence
--

ALTER TABLE ONLY num_types ALTER COLUMN id SET DEFAULT nextval('num_types_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: zilence
--

ALTER TABLE ONLY timestamp_types ALTER COLUMN id SET DEFAULT nextval('timestamp_types_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: zilence
--

ALTER TABLE ONLY types ALTER COLUMN id SET DEFAULT nextval('types_id_seq'::regclass);


--
-- Data for Name: array_types; Type: TABLE DATA; Schema: public; Owner: zilence
--

COPY array_types (id, aint2, aint4, aint8, atext, anumeric) FROM stdin;
\.


--
-- Name: array_types_id_seq; Type: SEQUENCE SET; Schema: public; Owner: zilence
--

SELECT pg_catalog.setval('array_types_id_seq', 736, true);


--
-- Data for Name: binary_types; Type: TABLE DATA; Schema: public; Owner: zilence
--

COPY binary_types (id, fbytea) FROM stdin;
\.


--
-- Name: binary_types_id_seq; Type: SEQUENCE SET; Schema: public; Owner: zilence
--

SELECT pg_catalog.setval('binary_types_id_seq', 251, true);


--
-- Data for Name: dummy; Type: TABLE DATA; Schema: public; Owner: zilence
--

COPY dummy (nnull) FROM stdin;
\.


--
-- Data for Name: num_types; Type: TABLE DATA; Schema: public; Owner: zilence
--

COPY num_types (id, fint2, fint4, fint8, fnumeric) FROM stdin;
\.


--
-- Name: num_types_id_seq; Type: SEQUENCE SET; Schema: public; Owner: zilence
--

SELECT pg_catalog.setval('num_types_id_seq', 355257, true);


--
-- Data for Name: timestamp_types; Type: TABLE DATA; Schema: public; Owner: zilence
--

COPY timestamp_types (id, ftimestamp, ftimestamptz, fdate) FROM stdin;
\.


--
-- Name: timestamp_types_id_seq; Type: SEQUENCE SET; Schema: public; Owner: zilence
--

SELECT pg_catalog.setval('timestamp_types_id_seq', 1425, true);


--
-- Data for Name: types; Type: TABLE DATA; Schema: public; Owner: zilence
--

COPY types (id, t_bool, t_int2, t_int4, t_int8, t_float4, t_float8, t_text, t_hstore, t_uuid) FROM stdin;
\.


--
-- Name: types_id_seq; Type: SEQUENCE SET; Schema: public; Owner: zilence
--

SELECT pg_catalog.setval('types_id_seq', 1097, true);


--
-- Name: array_types_pkey; Type: CONSTRAINT; Schema: public; Owner: zilence; Tablespace: 
--

ALTER TABLE ONLY array_types
    ADD CONSTRAINT array_types_pkey PRIMARY KEY (id);


--
-- Name: binary_types_pkey; Type: CONSTRAINT; Schema: public; Owner: zilence; Tablespace: 
--

ALTER TABLE ONLY binary_types
    ADD CONSTRAINT binary_types_pkey PRIMARY KEY (id);


--
-- Name: num_types_pkey; Type: CONSTRAINT; Schema: public; Owner: zilence; Tablespace: 
--

ALTER TABLE ONLY num_types
    ADD CONSTRAINT num_types_pkey PRIMARY KEY (id);


--
-- Name: types_pkey; Type: CONSTRAINT; Schema: public; Owner: zilence; Tablespace: 
--

ALTER TABLE ONLY types
    ADD CONSTRAINT types_pkey PRIMARY KEY (id);


--
-- Name: public; Type: ACL; Schema: -; Owner: zilence
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM zilence;
GRANT ALL ON SCHEMA public TO zilence;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

