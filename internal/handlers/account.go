package handlers

import (
	"time"

	"html/template"
	"net/http"

	"github.com/rs/xid"
	"go.uber.org/zap"
)

var CurrentTokens AuthenticationTokens

type AuthenticationTokens struct {
	// map with key of token and value of user email
	TokenUsers map[string]string
	Log        *zap.SugaredLogger
}

func init() {
	CurrentTokens = AuthenticationTokens{}
	CurrentTokens.TokenUsers = make(map[string]string)
}

func (u *HandlerWrapper) Logout(w http.ResponseWriter, r *http.Request) {

	u.Log.Info("logout called")

	c, err := r.Cookie("X-Session-Token")
	if err != nil {
		u.Log.Info(err.Error() + " in cookie search")
		http.Redirect(w, r, "/login", 302)
		return
	}

	token := c.Value

	// delete the cached token
	delete(CurrentTokens.TokenUsers, token)

	// delete the stored cookie
	http.SetCookie(w, &http.Cookie{
		Name:   "X-Session-Token",
		Value:  "",
		MaxAge: 0,
	})

	tmpl, err := template.ParseFiles("pages/logout.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	pageValues := PipelinesPage{}
	tmpl.ExecuteTemplate(w, "layout", &pageValues)
}

func (u *HandlerWrapper) Login(w http.ResponseWriter, r *http.Request) {

	u.Log.Info("login called")

	tmpl, err := template.ParseFiles("pages/login.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	pageValues := PipelinesPage{}
	tmpl.ExecuteTemplate(w, "layout", &pageValues)
}

func (u *HandlerWrapper) LoginTry(w http.ResponseWriter, r *http.Request) {

	u.Log.Info("logintry called")

	r.ParseForm()

	email := r.Form["email"][0]
	password := r.Form["pwd"][0]

	if email == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid email address")
		return
	}
	if password == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid password")
		return
	}

	u.Log.Infof("entered email %s password %s\n", email, password)

	// add to list of current tokens
	token := xid.New().String()
	CurrentTokens.TokenUsers[token] = email
	//r.Header.Set("X-Session-Token", token)
	http.SetCookie(w, &http.Cookie{
		Name:    "X-Session-Token",
		Value:   token,
		Expires: time.Now().Add(120 * time.Second),
	})
	u.Log.Infof("setting cookie...\n")

	// on valid login, redirect to home page
	http.Redirect(w, r, "/", 302)
}

func (u *HandlerWrapper) ProfileUpdate(w http.ResponseWriter, r *http.Request) {

	u.Log.Info("ProfileUpdate called")

	r.ParseForm()

	email := r.Form["email"][0]
	password := r.Form["pwd"][0]

	if email == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid email address")
		return
	}
	if password == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid password")
		return
	}

	u.Log.Infof("entered email %s password %s\n", email, password)

	// on valid login, redirect to home page
	http.Redirect(w, r, "/", 302)
}

func (u *HandlerWrapper) Profile(w http.ResponseWriter, r *http.Request) {

	u.Log.Info("Profile called")

	tmpl, err := template.ParseFiles("pages/profile.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	pageValues := PipelinesPage{}
	tmpl.ExecuteTemplate(w, "layout", &pageValues)
}

// Middleware function, which will be called for each request
func (amw *AuthenticationTokens) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		amw.Log.Infof("middleware URI=%s\n", r.RequestURI)
		if r.RequestURI == "/logout" || r.RequestURI == "/login" || r.RequestURI == "/logintry" {
			next.ServeHTTP(w, r)
			return
		}
		//token := r.Header.Get("X-Session-Token")
		c, err := r.Cookie("X-Session-Token")
		if err != nil {
			amw.Log.Errorf(err.Error() + " in cookie search")
			http.Redirect(w, r, "/login", 302)
			return
		}

		token := c.Value

		amw.Log.Infof("middleware token found in header of [%s]\n", token)

		if user, found := CurrentTokens.TokenUsers[token]; found {
			// We found the token in our map
			amw.Log.Infof("Authenticated user %s\n", user)
			// Pass down the request to the next middleware (or final handler)
			next.ServeHTTP(w, r)
		} else {
			// Write an error and stop the handler chain
			//		http.Error(w, "Forbidden", http.StatusForbidden)
			http.Redirect(w, r, "/login", 302)
		}
	})
}
