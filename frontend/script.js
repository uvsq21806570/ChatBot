const msg_box = document.querySelector(".msgBox");
const msg_input = document.getElementById("msgInput")
const chat = document.querySelector(".chat");

const botname = "PolluBot";
const username = "You";

function appendMsg(name, profile, text) {
    //   Simple solution for small apps
    const msgHTML = `
        <div class="msg ${profile}">
            <div class="bubble ${profile}">
                <div class="name ${profile}">${name}</div>
                <div class="text">${text}</div>
            </div>
        </div>
    `;

    chat.insertAdjacentHTML("beforeend", msgHTML);
    chat.scrollTop += 500;
}

function sendMessage() {
    const text = msg_input.value;
    if (!text) return;

    appendMsg(username, 'user', text);
    msg_input.value = "";
    botResponse(text);
}

function botResponse(userMessage) {
    var botMessage = "Sorry i didn't understand what you were asking."; //the default message

    if (userMessage === 'hi' || userMessage == 'hello') {
        const hi = ['hi', 'howdy', 'hello']
        botMessage = hi[Math.floor(Math.random() * (hi.length))];;
    }

    if (userMessage === 'name') {
        botMessage = 'My name is ' + botname;
    }

    appendMsg(botname, 'bot', botMessage);
}

document.onkeypress = keyPress;
//if the key pressed is 'enter' runs the function newEntry()
function keyPress(e) {
    var x = e || window.event;
    var key = (x.keyCode || x.which);
    if (key == 13 || key == 3) {
        //runs this function when enter is pressed
        sendMessage();
    }
}

// This function is set to run when the users brings focus to the messagebox, by clicking on it
function placeHolder() {
    document.getElementById("msgInput").placeholder = "";
}