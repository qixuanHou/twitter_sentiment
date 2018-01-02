function redirectSearch() {
  url = document.location.href;
  query = $("#searchBar").val();
  if(query != ""){
      window.location.replace(url + 'user/' + query);  
  }  
}

// function searchStart(){
//     if(event.keyCode == 13){
//         redirectSearch();
//     }
// }
document.addEventListener('keyup',function(e){
	if(e.keyCode == 13) {
    url = document.location.href;
    query = $("#searchBar").val();
		if(query != "") {
			console.log(url + 'user/' + query);
			window.location.replace(url + 'user/' + query);

		}
	}
});