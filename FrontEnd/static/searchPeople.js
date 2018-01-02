function peopleSearch() {
  url = document.location.href;
  query = $("#searchBar").val();
  url = url.split("/")[0];
  window.location.replace(url + query);  
}
